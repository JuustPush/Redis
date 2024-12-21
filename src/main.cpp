#include "Server.h"
#include <asio/io_context.hpp>
#include <exception>
#include <iostream>

struct Argument {
  int16_t port{kDefaultPort};
  
  std::optional<std::string> replicaof_host;
  std::optional<int16_t> replicaof_port;
  std::optional<std::string> dir_path;
  std::optional<std::string> dbfilename;

  static constexpr int16_t kDefaultPort = 6379;

  ServerConfig to_server_config() {
    ServerConfig config;
    config.port = port;
    if (replicaof_port.has_value()) {
      config.replicaof = {replicaof_host.value(), replicaof_port.value()};
    }
    if (dir_path.has_value()){
      config.dir_path=dir_path.value();
      config.dbfilename=dbfilename.value();
    }
    
    return config;
  }
};

Argument read_argument(int argc, char **argv) {
  // Command line arguments will be stored in a map
  std::unordered_map<std::string, std::string> args;

  // Parse the command line arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.substr(0, 2) == "--" && i + 1 < argc) {
      args[arg.substr(2)] = argv[++i];
    }
  }

  Argument argument;
  // Check if the port argument was provided
  if (auto it = args.find("port"); it != args.end()) {
    int port = std::stoi(it->second);
    argument.port = port;
  }

  // Check if replicaof argument was provided
  if (auto it = args.find("replicaof"); it != args.end()) {
    std::istringstream iss(it->second);
    std::string host;
    int64_t port;
    iss >> host;
    iss >> port;
    argument.replicaof_host = std::move(host);
    argument.replicaof_port = port;
  }
  if (auto it = args.find("dir"); it != args.end()){
    std::string path = it->second;
    std::cout<<path<<std::endl;
    argument.dir_path=path;
  }
  if (auto it =args.find("dbfilename"); it != args.end()){
    std::string dbfn = it->second;
    std::cout<<dbfn<<std::endl;
    argument.dbfilename = dbfn;
  }
  
  return argument;
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::cout << "Logs from your program will appear here!\n";

  auto argument = read_argument(argc, argv); 
  auto server_config = argument.to_server_config();

  try {
    asio::io_context io_context;
    Server server(io_context, server_config);
    io_context.run();
  } catch (std::exception &e) {
    std::cout << "Exception: " << e.what() << "\n";
  }

  return 0;
}