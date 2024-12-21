#include "Wait.h"
#include "Helpers.h"
#include "Parser.h"
#include "Replica.hpp"
#include "Session.h"
#include <asio/basic_stream_socket.hpp>
#include <asio/buffer.hpp>
#include <asio/buffers_iterator.hpp>
#include <asio/read.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <iostream>
#include <optional>

namespace commands {

void Wait::handle_wait(
    std::shared_ptr<Replica> replica, std::shared_ptr<int> needed,
    std::shared_ptr<int> acks, int64_t deadline, Session *client_session,
    std::shared_ptr<asio::steady_timer> timer,
    std::vector<std::shared_ptr<asio::steady_timer>> timers) {
  auto replica_session = replica->get_session();
  auto &socket = replica_session->get_socket();
  static const std::string command =
      Parser::encodeRespArray({"REPLCONF", "GETACK", "*"});
  socket.cancel();
  asio::async_write(
      socket, asio::buffer(command),
      [this, replica, replica_session, needed, deadline,
       client_session = client_session->shared_from_this(), timer, timers,
       acks](const asio::error_code &ec, size_t len) {
        auto &socket = replica_session->get_socket();
        int expected_offset = replica->get_expected_offset();
        replica->add_expected_offset(command.size());
        std::cout << "Wait command: waiting for read\n";
        auto buffer = std::make_shared<std::array<char, 2014>>();
        socket.async_read_some(
            asio::buffer(*buffer),
            [this, replica, buffer, needed, expected_offset, deadline,
             client_session, acks, timer,
             timers](const asio::error_code &ec, size_t len) {
              if (ec) {
                std::cout << "Wait comamnd, Error = " << ec.message() << "\n";
                (*needed)--;
              } else {
                std::string response(buffer->begin(), buffer->begin() + len);
                auto parsed = Parser::decode(response);
                auto offset = std::stoi(parsed[0].back());
                if (offset >= expected_offset) {
                  (*needed)--;
                  (*acks)++;
                } else {
                  handle_wait(replica, needed, acks, deadline,
                              client_session.get(), timer, timers);
                  return;
                }
              }
              std::cout << "Needed = " << *needed << "\n";
              if (*needed == 0) {
                for (auto &r_timer : timers) {
                  r_timer->cancel();
                }
                std::string response = ":" + std::to_string(*acks) + "\r\n";
                write(std::move(response), client_session.get());
              }
              timer->cancel();
              client_session->start();
            });
      });
}

void Wait::handle(const std::vector<std::string> &command_list,
                  Session *session) {
  std::span<const std::string> params{command_list.begin() + 1,
                                      command_list.end()};

  inner_handle(params, session);
}

std::optional<std::string>
Wait::inner_handle(const std::span<const std::string> &params,
                   Session *session) {
  try {
    int min_replicas = std::stoi(params[0]);
    int timeout_ms = std::stoi(params[1]);
    auto replicas = session->get_replicas();

    if (min_replicas == 0) {
      write(":0\r\n", session);
      return std::nullopt;
    }

    auto deadline = get_current_timestamp_ms() + timeout_ms;

    int avail_replicas = 0;
    for (const auto &replica : replicas) {
      avail_replicas += !replica->get_session()->is_session_closed();
    }

    auto needed = std::make_shared<int>(std::min(min_replicas, avail_replicas));
    auto acks = std::make_shared<int>(0);

    std::vector<std::shared_ptr<asio::steady_timer>> timers;
    for (auto &replica : replicas) {
      auto replica_session = replica->get_session();
      if (replica_session->is_session_closed()) {
        continue;
      }
      if (replica->get_expected_offset() == 0) {
        (*acks)++;
        continue;
      }
      auto timer = std::make_shared<asio::steady_timer>(
          replica_session->get_io_context());
      timer->expires_after(std::chrono::milliseconds(timeout_ms));
      timer->async_wait([replica_session](const asio::error_code &ec) {
        auto &socket = replica_session->get_socket();
        socket.cancel();
      });
      timers.emplace_back(std::move(timer));
    }
    if (timers.empty()) {
      write(":" + std::to_string(*acks) + "\r\n", session);
    } else {
      int i = 0;
      for (auto &replica : replicas) {
        auto replica_session = replica->get_session();
        if (replica_session->is_session_closed() ||
            replica->get_expected_offset() == 0) {
          continue;
        }
        handle_wait(replica, needed, acks, deadline, session,
                    timers[i++], timers);
      }
    }

  } catch (std::exception &e) {
    write(std::nullopt, session);
  }
  return std::nullopt;
}

} // namespace commands