#pragma once

#include "CommandHandler.h"
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/write.hpp>
#include <atomic>
#include <memory>

using asio::ip::tcp;

class Server;
class Replica;
class ReplicaManager;

class Session : public std::enable_shared_from_this<Session> {
  friend class CommandHandler;

public:
  Session(asio::io_context &io_context, Server *server, bool is_master = false);

  tcp::socket &get_socket();

  asio::io_context &get_io_context();

  void start();

  template <typename F>
  void write(std::string message, F callback, bool start_read = true) {
    std::shared_ptr<std::string> shared_message =
        std::make_shared<std::string>(std::move(message));
    asio::async_write(
        socket_, asio::buffer(*shared_message),
        [this, self = shared_from_this(), shared_message, callback,
         start_read](const asio::error_code &error_code, size_t len) {
          callback(error_code, len);
          if (start_read) {
            start();
          }
        });
  }

  void set_as_replica();

  bool is_master_session() const;

  std::vector<std::shared_ptr<Replica>> get_replicas() const;

  bool is_session_closed() const;

  void handle_message(const std::string message);

  void add_expected_offset(int delta);

  int get_expected_offset() const;

protected:
  void handle_read(const asio::error_code &error_code, size_t len);

  void handle_write(const asio::error_code &error_code, size_t len);

  static constexpr size_t kBufferSize = 1024;

  asio::io_context &io_context_;
  tcp::socket socket_;
  char data_[kBufferSize];
  CommandHandler command_handler_;
  bool is_master_session_{false};
  bool is_closed{false};
  std::shared_ptr<ReplicaManager> replicas_;
  std::atomic<int> expected_offset{0};
};