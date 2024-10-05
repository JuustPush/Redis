#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <vector>
#include <map>
#include <chrono>
#include <thread>
#include <pthread.h>
#include <fstream>
#include <cassert>
std::map<std::string, std::string> kv;
std::map<std::string, int64_t> valid_until_ts;
std::string hex_empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
int master_port = -1; // -1 -> master
std::string master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int master_repl_offset = 0;
std::vector<int> replicas_fd;
bool handshake_complete  = false;
int count =0;
std::string dir;
std::string dbfilename;

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
uint8_t read(std::ifstream &rdb)
{
	uint8_t val;
	rdb.read(reinterpret_cast<char *>(&val), sizeof(val));
	return val;
}
std::pair<std::optional<uint64_t>, std::optional<int8_t>> get_str_bytes_len(std::ifstream &rdb){
    auto byte = read(rdb);
	// Get the two most significant bits of the byte
	// These bits determine how the length is encoded
	auto sig = byte >> 6; // 0 bytes, 1, 2, 3 - 00, 01, 10, 11
	switch (sig) {
		case 0:
		{
			// If the two most significant bits are 00
			// The length is the lower 6 bits of the byte
			return {byte & 0x3F, std::nullopt};
		}
		case 1:
		{
			// If the two most significant bits are 01
			// The length is the lower 6 bits of the first byte and the whole next byte
			auto next_byte = read(rdb);
			uint64_t sz = ((byte & 0x3F) << 8) | next_byte;
			return {sz, std::nullopt};
		}
		case 2:
		{
			// If the two most significant bits are 10
			// The length is the next 4 bytes
			uint64_t sz = 0;
			for (int i = 0; i < 4; i++) {
				auto byte = read(rdb);
				sz = (sz << 8) | byte;
			}
			return {sz, std::nullopt};
		}
		case 3:
		{
			// If the two most significant bits are 11
			// The string is encoded as an integer
			switch (byte)
			{
			case 0xC0:
				// The string is encoded as an 8-bit integer of 1 byte
				return {std::nullopt, 8};
			case 0xC1:
				// The string is encoded as a 16-bit integer of 2 bytes
				return {std::nullopt, 16};
			case 0xC2:
				// The string is encoded as a 32-bit integer of 4 bytes
				return {std::nullopt, 32};
			case 0xFD:
				// Special case for database sizes
				return {byte, std::nullopt};
			default:
				return {std::nullopt, 0};
			}
		}
	}
	return {std::nullopt, 0};
}
std::string read_byte_to_string(std::ifstream &rdb)
{
	std::pair<std::optional<uint64_t>,std::optional<int8_t>> decoded_size=get_str_bytes_len(rdb);
    if (decoded_size.first.has_value()){
        int size = decoded_size.first.value();
        std::vector<char> buffer(size);
        rdb.read(buffer.data(),size);
        return std::string(buffer.data(),size);
    }
    assert(decoded_size.second.has_value());
    int type = decoded_size.second.value();
    switch(type){
        case 8:
        {
            int8_t val;
            rdb.read(reinterpret_cast<char*>(&val),sizeof(val));
            return std::to_string(val);
        }
        case 16:
        {
            int16_t val;
            rdb.read(reinterpret_cast<char*>(&val),sizeof(val));
            //val=be16toh(val);
            return std::to_string(val);
        }
        case 32:
        {
            int32_t val;
            rdb.read(reinterpret_cast<char*>(&val),sizeof(val));
            //val = be32toh(val);
            return std::to_string(val);
        }
    }
	
    return "";
}
void initializeKeyValues(){
    if(dir.empty() || dbfilename.empty()) return;
    std::string rdbFullPath = dir + "/" +dbfilename;
    std::ifstream rdb(rdbFullPath, std::ios_base::binary);
    if (!rdb.is_open()){
        std::cout << "Could not open the Redis Persistent Database:"  << rdbFullPath << std::endl;
		return;
    }
    char header[9];
    rdb.read(header,9);
    std::cout << "Header: " << std::string(header, 9) << std::endl; //always REDIS0011
    while (true){
        unsigned char opcode;
        if (!rdb.read(reinterpret_cast<char*>(&opcode),1))
            throw std::runtime_error("Reached end of file while looking for database start");
        if (opcode == 0xFA) // some metadata like version
		{
			std::string key = read_byte_to_string(rdb);
			std::string value = read_byte_to_string(rdb);
			std::cout << "Info: " << key << " " << value << std::endl;
		}
        if (opcode == 0xFE)
		{
			auto db_number = get_str_bytes_len(rdb);
			if (db_number.first.has_value())
			{
				std::cout << "SELECTDB: Database number: " << db_number.first.value() << std::endl;
				opcode = read(rdb); // Read next opcode
			}
		}
        if (opcode == 0xFB)
		{
			auto hash_table_size = get_str_bytes_len(rdb);
			auto expire_hash_table_size = get_str_bytes_len(rdb);
			if (hash_table_size.first.has_value() &&
				expire_hash_table_size.first.has_value())
			{
				std::cout << "Hash table size: " << hash_table_size.first.value() << " "
							<< "Expiry hash table size: " << expire_hash_table_size.first.value() << std::endl;
			}
			break;
		}
    }
    while (true){
        unsigned char opcode;
		if (!rdb.read(reinterpret_cast<char*>(&opcode), 1))
		{
			std::cout << "Reached end of file" << std::endl;
			break;
		}
        if (opcode == 0xFF)
		{
			std::cout << "Reached end of database" << std::endl;
			uint64_t checksum;
			rdb.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
			//checksum = be64toh(checksum); // be is big endian to host order
			std::cout << "DB checksum: " << checksum << std::endl;
			
			// Exit while loop
			break;
		}
        uint64_t expire_time_s = 0;
		uint64_t expire_time_ms = 0;
        if (opcode == 0xFD)
		{
        // expiry time in seconds followed by 4 byte - uint32_t
        uint32_t seconds;
        rdb.read(reinterpret_cast<char*>(&seconds), sizeof(seconds));
        //expire_time_s = be32toh(seconds);
        std::cout << "EXPIRETIME: " << expire_time_s << std::endl;
        rdb.read(reinterpret_cast<char*>(&opcode), 1);
		}
        if  (opcode == 0xFC)
		{
        // expiry time in ms, followd by 8 byte unsigned - uint64_t
        rdb.read(reinterpret_cast<char*>(&expire_time_ms), sizeof(expire_time_ms));
        //expire_time_ms = be32toh(expire_time_ms);
        std::cout << "EXPIRETIME ms: " << expire_time_ms << std::endl;
        rdb.read(reinterpret_cast<char*>(&opcode), 1);
        }
        std::string key = read_byte_to_string(rdb);
        std::string value = read_byte_to_string(rdb);

        std::chrono::_V2::system_clock::time_point expiry_time;
        // if (expire_time_ms > 0) expiry_time = std::chrono::system_clock::from_time_t((expire_time_ms/1000)); //std::chrono::milliseconds(expire_time_ms) 
        // else expiry_time = std::chrono::time_point<std::chrono::system_clock>::max();
        kv[key]=value;
        if (expire_time_ms > 0) valid_until_ts[key]=expire_time_ms;
        //kVars.emplace(std::string(key), Value{std::string(value), expiry_time});
        
    }
    rdb.close();
}


std::unique_ptr<std::vector<std::string>> getAllKeys(const std::string_view& regex)
{
    auto result{std::make_unique<std::vector<std::string>>()};
    std::cout << "Got regex: " << regex << std::endl;
	for (const auto& key: kv)
	{
    std::cout<<key.first<<std::endl;
		    result->push_back(key.first);
	}
	return result;
}



void recv_repl_id(int master_fd) 
{
  // can be extended to read the repl_id and repl_offset value
  char* buf;
  while(buf[0] != '\n') 
  {
    recv(master_fd, buf, 1, 0);
  }
}
void recv_rdb_file(int master_fd) 
{
  char buf[1024];
  bool size_determined = false;
  int size = 0;
  while(!size_determined) 
  {
    recv(master_fd, buf, 1, 0);
    std::cout << buf[0] <<"\n";
    if(buf[0] == '$' || buf[0] == '\r') 
      continue;
    else if (buf[0] == '\n')
      size_determined = true;
    else 
      size = size * 10 + (buf[0] - '0');
  }
  std::cout << size << "\n";
  int bytes_recvd = 0;
  while(bytes_recvd < size)
  {
    recv(master_fd, buf, 1, 0);
    std::cout << buf[0];
    bytes_recvd++;
  }
}
int64_t get_current_timestamp()
{
  return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
std::string hex_to_string(const std::string &hex) {
    std::string result;
    for(int i=0; i<hex.length(); i+=2) {
        unsigned int byte;
        std::stringstream ss;
        ss << std::hex << hex.substr(i, 2);
        ss >> byte;
        result.push_back(static_cast<char>(byte));
    }
    return result;
}
std::vector<std::string> input_tokenizer(std::string buf)
{
  std::vector<std::string> in_tokens;
  std::string curr_arr_el = "";
  for (int i = 0; i < buf.size(); i++)
  {
    if (buf[i] != '\r' && buf[i] != '\n')
      curr_arr_el += buf[i];
    else if (buf[i] == '\n')
    {
      in_tokens.push_back(curr_arr_el);
      curr_arr_el = "";
    }
  }
  return in_tokens;
}
std::vector<std::string> protocol_parser(std::string buf)
{
  int len = buf.size();
  std::vector<std::string> parse_result;
  std::vector<std::string> in_tokens = input_tokenizer(buf);
  int total_no_of_out_tokens;
  int max_size_of_next_token;
  for (auto s : in_tokens)
  {
    if (s[0] == '*' && s.size()>1)
      total_no_of_out_tokens = stoi(s.substr(1));
    else if (s[0] == '$')
      max_size_of_next_token = stoi(s.substr(1));
    else
    {
      if (s.size() != max_size_of_next_token)
        std::cerr << "Invalid command\n";
      parse_result.push_back(s);
    }
    if(parse_result.size() == total_no_of_out_tokens) 
      break;
  }
  if (parse_result.size() != total_no_of_out_tokens)
    std::cerr << "Invalid command\n";
  return parse_result;
}
std::string token_to_resp_bulk(std::string token)
{
  if (token.empty())
    return "$-1\r\n";
  return "$" + std::to_string(token.size()) + "\r\n" + token + "\r\n";
}
void send_string_wrap(int client_fd, std::string msg)
{
  std::string resp_bulk = token_to_resp_bulk(msg);
  // std::cout << resp_bulk << "\n";
  char *buf = resp_bulk.data();
  send(client_fd, buf, resp_bulk.size(), 0);
}
void send_string_vector_wrap(int client_fd, std::vector<std::string> msgs)
{
  std::string combined_resp = "*" + std::to_string(msgs.size()) + "\r\n";
  for (std::string str : msgs)
  {
    combined_resp += token_to_resp_bulk(str);
  }
  char *buf = combined_resp.data();
  send(client_fd, buf, combined_resp.size(), 0);
}
void send_rdb_file_data(int client_fd, std::string hex)
{
  std::string bin = hex_to_string(hex);
  std::string resp = "$" + std::to_string(bin.size()) + "\r\n" + bin;
  std::cout << resp << "\n";
  char *buf = resp.data();
  send(client_fd, buf, resp.size(), 0);
}
void handle_client(int client_fd)
{
  char client_command[1024] = {'\0'};
  while (recv(client_fd, client_command, sizeof(client_command), 0) > 0)
  {
    std::string string_buf{client_command};
    std::cout << string_buf << "\n";
    for (int i = 0; i < string_buf.size(); i++)
      string_buf[i] = tolower(string_buf[i]);
      std::vector<int> resp_arr_starting_idx;
for(int i = 0; i < string_buf.size(); i++) 
    {if(string_buf[i] == '*') 
        resp_arr_starting_idx.push_back(i);
    }
     if(resp_arr_starting_idx.empty())
      resp_arr_starting_idx.push_back(0);
    for(int i = 0; i < resp_arr_starting_idx.size(); i++) 
    {
       int resp_arr_len;
      if(i != resp_arr_starting_idx.size() - 1)
      {
        resp_arr_len = resp_arr_starting_idx[i + 1] - resp_arr_starting_idx[i];
      }
      else 
      {
        resp_arr_len = string_buf.size() - resp_arr_starting_idx[i];
      }
      count+=resp_arr_len;
      std::cout << resp_arr_starting_idx[i] << " " << resp_arr_len << "\n";
      auto parsed_in = protocol_parser(string_buf.substr(resp_arr_starting_idx[i], resp_arr_len));
      std::string command = parsed_in[0];
      std::cout<<"current command - "<<command<<std::endl;
 
      if (command == "ping")
      {
        if(master_port==-1)
        send_string_wrap(client_fd, "PONG");
      }
       else if (command == "echo")
      {
        send_string_wrap(client_fd, parsed_in[1]);
      }
      else if (command == "set")
      {
         std::string key = parsed_in[1];
        std::string value = parsed_in[2];
        kv[key] = value;
        if (parsed_in.size() > 3)
        {
           if (parsed_in[3] == "px")
          {
            valid_until_ts[key] = get_current_timestamp() + (int64_t)stoi(parsed_in[4]);
          }
        }
         if(master_port == -1) 
        { send_string_wrap(client_fd, "OK");
        for(auto fd : replicas_fd)
          {
            std::cout << fd << "propagated\n";
            send_string_vector_wrap(fd, parsed_in);
          }
        }
      }
      else if (command == "keys")
      {
        auto ptr = getAllKeys("*");
          std::vector<std::string>* rgx = ptr.get();
          std::string response="*"+std::to_string(rgx->size())+"\r\n";

          
          for (const auto& m : *rgx){
              response+="$"+std::to_string(m.length())+"\r\n"+m+"\r\n";
              std::cout<<response<<std::endl;
              
          }
          send(client_fd,response.data(),response.length(),0);
      }
      else if (command == "config"){
            std::string info = parsed_in[2];
            std::string response="*2\r\n";
            if (info=="dir"){
                response+="$3\r\ndir\r\n";
                response+="$" + std::to_string(dir.size()) + "\r\n" + dir + "\r\n";
            }
            else if (info=="dbfilename"){
                response += "$10\r\ndbfilename\r\n";
                response += "$" + std::to_string(dbfilename.size()) + "\r\n" + dbfilename + "\r\n";
            }
            send(client_fd,response.data(),response.length(),0);
        } 


      else if (command == "get")
      {
        std::string key = parsed_in[1];
        if (!kv.contains(key) || (valid_until_ts.contains(key) && get_current_timestamp() > valid_until_ts[key]))
          send_string_wrap(client_fd, "");
        else
        send_string_wrap(client_fd, kv[key]);
      }
      else if (command == "info")
      {
        int args = parsed_in.size() - 1;
         if (args == 1 && parsed_in[1] == "replication")
        {
          if (master_port == -1)
          {
            std::string resp = "role:master\r\nmaster_replid:" + master_repl_id + "\r\nmaster_repl_offset:" + std::to_string(master_repl_offset);
            send_string_wrap(client_fd, resp);
          }
          else
            send_string_wrap(client_fd, "role:slave");
        }
      }
      else if (command == "replconf")
      {
        if(master_port != -1 && handshake_complete && parsed_in[1] == "getack") 
        {
          std::cout << "Hello there!";
          send_string_vector_wrap(client_fd, {"REPLCONF", "ACK", std::to_string(count-resp_arr_len)});
        }
        else 
        {
          send(client_fd, "+OK\r\n", 5, 0);
          if(replicas_fd.empty() || replicas_fd.back() != client_fd)
            replicas_fd.push_back(client_fd);
        }
      
      }
      else if (command == "psync")
      {
        std::string recv_master_id = parsed_in[1];
        int recv_master_offset = stoi(parsed_in[2]);
        if (recv_master_id == "?" && recv_master_offset == -1)
        {
          std::string resp = "+FULLRESYNC " + master_repl_id + " " + std::to_string(master_repl_offset) + "\r\n";
          send(client_fd, resp.data(), resp.size(), 0);
          send_rdb_file_data(client_fd, hex_empty_rdb);
        }
      }
      else if (command == "wait")
      {
        std::string resp = ":0\r\n";
        send(client_fd,resp.data(),resp.size(),0);
        int timeout = stoi(parsed_in[2]);
        // this_thread::sleep_for(chronos::duration::milliseconds(timeout));
      }
      
    }
    for (int i = 0; i < sizeof(client_command); i++)
      client_command[i] = '\0';
  }
  close(client_fd);
}
int main(int argc, char **argv)
{
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";
  // std::cout << "Protocol parser test\n";
  // auto test_result = protocol_parser("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  // for (auto s : test_result)
  //   std::cout << s << "\n";
  int self_port = 6379;
  if (argc > 1)
  {
    if (strcmp(argv[1], "--port") == 0)
    {
      std::string port_in{argv[2]};
      self_port = stoi(port_in);
    }
  }
    // Uncomment this block to pass the first stage
  //
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }
 int master_fd;
 if(master_port == -1) 
    {
       if (argc > 1) {
        for (int i = 1; i < argc; ++i) {
            auto cmd = std::string_view(argv[i]);
            if (cmd == "--port") {
                //port = std::stoi(argv[++i]);
            } else if (cmd == "--replicaof") {
                auto param = std::string(argv[++i]);
                auto pos = param.find(' ');
                //master_host = param.substr(0, pos);
                //std::string strPort=param.substr(pos + 1);
                master_port = std::stoi(param.substr(pos + 1));
                argv[i][pos] = '\0';
                //kIsMaster = false;
            } else if (cmd == "--dir"){
                dir = argv[++i];
                continue;
            } else if (cmd == "--dbfilename"){
                dbfilename = argv[++i];
                initializeKeyValues();
            }
        }
    }
       if (master_port != -1)
      {
          master_fd = socket(AF_INET, SOCK_STREAM, 0);
           struct sockaddr_in master_addr;
          master_addr.sin_family = AF_INET;
          master_addr.sin_port = htons(master_port);
          master_addr.sin_addr.s_addr = INADDR_ANY;
          if (connect(master_fd, (struct sockaddr *)&master_addr, sizeof(master_addr)) == -1)
          {
            std::cerr << "Replica failed to connect to master\n";
          }
          char buf[1024] = {'\0'};
          send_string_vector_wrap(master_fd, {"ping"});
          recv(master_fd, buf, sizeof(buf), 0);
          memset(buf, 0, 1024);
          send_string_vector_wrap(master_fd, {"REPLCONF", "listening-port", std::to_string(self_port)});
          recv(master_fd, buf, sizeof(buf), 0);
          memset(buf, 0, 1024);
          send_string_vector_wrap(master_fd, {"REPLCONF", "capa", "psync2"});
          recv(master_fd, buf, sizeof(buf), 0);
          memset(buf, 0, 1024);
          send_string_vector_wrap(master_fd, {"PSYNC", "?", "-1"});
          recv_repl_id(master_fd);
          recv_rdb_file(master_fd);
          memset(buf, 0, 1024);
          handshake_complete = true;
 std::thread t(handle_client, master_fd);
          t.detach();
        }
    }
// Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
  {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(self_port);
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0)
  {
    std::cerr << "listen failed\n";
    return 1;
  }
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";
  std::vector<std::thread> threads;
  int client_fd;
  while (true)
  {
    client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    std::cout << "Client connected\n";
  threads.push_back(std::thread(handle_client, client_fd));
  }
  for(auto &t : threads) 
  {
    t.join();
  }
  close(server_fd);
  return 0;
}