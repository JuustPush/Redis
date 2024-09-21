#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <thread>
#include <vector>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>

#define BUFFER_SIZE 1024
#define MAX_CONNECTIONS 30
std::vector<std::string> splitRedisCommand(std::string input, std::string separator, int separatorLength) {
  std::vector<std::string> res;
  std::size_t foundSeparator = input.find(separator);
  if (foundSeparator == std::string::npos) {
    res.push_back(input);
  }
  while (foundSeparator != std::string::npos) {
    std::string splitOccurrence = input.substr(0, foundSeparator);
    res.push_back(splitOccurrence);
    input = input.substr(foundSeparator + separatorLength);
    foundSeparator = input.find(separator);
  }
  return res;
}
std::unordered_map<std::string, std::string> dictionary = {};
std::unordered_map<std::string,long> expTime;
void handle_connection(int client) {
  std::cout << "Client connected" << client << "\n";
  char buffer[BUFFER_SIZE];
  int bytes_read;
  const char* pong_response = "+PONG\r\n";
  while ((bytes_read = read(client, buffer, BUFFER_SIZE-1))>0){
    buffer[bytes_read] = '\0'; //null terminate the buffer
    std::cout << "received: " << buffer << std::endl;
    std::string input(buffer, strlen(buffer));
    std::vector<std::string> tokens = splitRedisCommand(input, "\r\n", 2);
    std::string cmd = "";
    for (const auto& x: tokens[2]){
        cmd += tolower(x);
    }
    if (cmd == "ping"){
        send(client, pong_response, strlen(pong_response), 0);
    } else if (cmd == "echo") {
        std::string echo_res = tokens[3] + "\r\n" + tokens[4] + "\r\n";
        send(client, echo_res.data(), echo_res.length(), 0);
    } else if (cmd == "set"){
        dictionary[tokens[4]] = tokens[6];
        if (tokens.size()>6 && tokens[8]=="px"){
            auto now = std::chrono::system_clock::now();
            auto now_in_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
            auto value = now_in_ms.time_since_epoch();
            long current_time_in_ms = value.count();
            expTime[tokens[4]]=current_time_in_ms+std::stoi(tokens[10]);
        }
        else {
            expTime[tokens[4]]=-1;
        }
        send(client, "+OK\r\n", 5, 0);
    } else if (cmd == "get"){
      if (dictionary.find(tokens[4]) == dictionary.end()){
        send(client, "$-1\r\n", 5, 0);
      } else {
        std::string g_response = "$" + std::to_string(dictionary[tokens[4]].size()) + "\r\n" + dictionary[tokens[4]] + "\r\n";
        auto now = std::chrono::system_clock::now();
        auto now_in_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        auto value = now_in_ms.time_since_epoch();
        long current_time_in_ms = value.count();
        if (dictionary.find(tokens[4])!=dictionary.end() && expTime[tokens[4]]==-1 || expTime[tokens[4]]>current_time_in_ms)
            send(client, g_response.data(), g_response.length(), 0);
        else {
            g_response="$-1\r\n";
            send(client, g_response.data(), g_response.length(), 0);
        }
      }
    }
  }
  close(client);
}
int main(int argc, char **argv) {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  // std::cout << "Logs from your program will appear here!\n";
  // Uncomment this block to pass the first stage
  //
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  //
  // // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  //
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  //
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  //
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  //
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  
  std::cout << "Waiting for a client to connect...\n";
  
  int n_connections = 0;
  do {
    int client_fd;
    client_fd = accept(server_fd,(struct sockaddr*) &client_addr, (socklen_t *)&client_addr_len);
    std::thread t(handle_connection, client_fd);
    t.detach();
    ++n_connections;
  } while (n_connections < MAX_CONNECTIONS);
  close(server_fd);
  return 0;
}