#pragma once

#include "ReplicationInfo.h"
#include "Storage.h"
#include <asio/error_code.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <mutex>
#include <shared_mutex>

using asio::ip::tcp;

class Session;
class Replica;
class ReplicaManager;



struct ServerConfig {
  int16_t port;
  std::string dir_path;
  std::string dbfilename;
  struct ReplicaOf {
    std::string host;
    int16_t port;
  };
  std::optional<ReplicaOf> replicaof;
};


class Server {
  friend class Session;

public:
  Server(asio::io_context &io_context, const ServerConfig &config);

private:
  void start_accept();

  void initializeKeyValues(const ServerConfig &config);

  void handle_accept(std::shared_ptr<Session> session,
                     const asio::error_code &error_code);

  std::shared_ptr<ReplicationInfo>
  init_replication_info(const ServerConfig &config);

  bool master_handshake(const ServerConfig &config);

  tcp::acceptor acceptor_;
  asio::io_context &io_context;
  std::shared_ptr<KVStorage> data_;
  std::shared_ptr<ReplicationInfo> replication_info_;
  std::shared_ptr<ReplicaManager> replica_manager_;
  std::shared_ptr<Session> master_session_;
};