#include "Session.h"
#include "CommandHandler.h"
#include "Server.h"
#include "Parser.h"
#include "Helpers.h"
#include "Replica.hpp"
#include <thread>
#include <iostream>
#include <memory>
#include <asio/write.hpp>
#include <asio/read.hpp>
#include <asio/streambuf.hpp>
#include <asio/read_until.hpp>
#include <asio/use_future.hpp>
#include <future>

Session::Session(asio::io_context &io_context, Server *server, bool is_master)
    : io_context_(io_context), socket_(io_context),
      command_handler_(server->data_, server->replication_info_, this),
      is_master_session_(is_master), replicas_(server->replica_manager_) {}

tcp::socket &Session::get_socket() { return socket_; }

asio::io_context &Session::get_io_context()
{
  return io_context_;
}

void Session::start()
{
  auto shared_self = shared_from_this();
  std::cout << "Start a read\n";
  socket_.async_read_some(
      asio::buffer(data_),
      [this, shared_self](const asio::error_code &error_code, size_t len)
      {
        handle_read(error_code, len);
      });
}

void Session::handle_read(const asio::error_code &error_code, size_t len)
{
  if (!error_code)
  {
    // process the received message
    std::string message(data_, len);

    handle_message(message);
  }
  else if (error_code == asio::error::eof)
  {
    std::cout << "Connection closed by peer\n";
  }
  else if (error_code == asio::error::operation_aborted)
  {
    std::cout << "Operation aborted\n";
  }
  else
  {
    // is_closed = true; // TODO: see where we can set is is_closed
    std::cout << "Error: " << error_code.message() << "\n";
  }
}

void Session::handle_write(const asio::error_code &error_code, size_t len)
{
  if (!error_code)
  {
    start();
  }
  else
  {
    std::cout << "Read failed, error = " << error_code.message() << "\n";
  }
}

void Session::set_as_replica()
{
  replicas_->add_replica(shared_from_this());
}

bool Session::is_master_session() const { return is_master_session_; }

std::vector<std::shared_ptr<Replica>> Session::get_replicas() const
{
  return replicas_->get_replicas();
}

bool Session::is_session_closed() const
{
  return is_closed;
}

void Session::handle_message(const std::string message)
{
  std::cout << "Debug: receive message = " << message << "\n";

  command_handler_.handle_raw_command(message);
}