#include "Command.h"
#include "Parser.h"
#include "Replica.hpp"
#include "Session.h"
#include <asio/write.hpp>
#include <iostream>

namespace commands {
void Command::handle(const std::vector<std::string> &command_list,
                     Session *session) {

  std::span<const std::string> params{command_list.begin() + 1,
                                      command_list.end()};

  std::optional<std::string> return_message = inner_handle(params, session);

  write(std::move(return_message), session);

  after_write(params, session);

  propagate(command_list, session);
}

void Command::default_call_back(const asio::error_code &error_code,
                                size_t len) {
  if (error_code) {
    std::cout << "error = " << error_code.message() << "\n";
  }
}

void Command::after_write(const std::span<const std::string> &params,
                          Session *session) {
  return;
}

void Command::write(std::optional<std::string> &&response, Session *session) {
  if (!is_write_command || !session->is_master_session()) {
    // don't write back if it's from master
    if (response.has_value()) {
      std::cout << "Return: " << response.value() << "\n";
      session->write(response.value(), default_call_back);
    } else {
      session->write(kErrorReturn, default_call_back);
    }
  } else {
    session->start();
  }
}

void Command::propagate(const std::vector<std::string> &command_list,
                        Session *session) {
  if (is_propagate) {
    const auto replicas = session->get_replicas();
    for (auto &replica : replicas) {
      auto replica_session = replica->get_session();
      if (replica_session->is_session_closed()) {
        continue;
      }
      auto raw_command = Parser::encodeRespArray(command_list);
      asio::async_write(replica_session->get_socket(),
                        asio::buffer(raw_command),
                        [](const asio::error_code &error_code, size_t len) {
                          if (error_code) {
                            std::cout
                                << "Failed to propagate to replicas, error = "
                                << error_code.message() << std::endl;
                          }
                        });
      replica->add_expected_offset(raw_command.size());
    }
  }
}
} // namespace commands