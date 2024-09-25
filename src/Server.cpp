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
#include <map>
#include <fstream>
#include <cassert>

#define BUFFER_SIZE 1024
#define MAX_CONNECTIONS 30

std::string dir;
std::string dbfilename;
std::map<std::string,std::string> m_mapKeyValues;
std::map<std::string, timeval> m_mapKeyTimeouts;


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

        timeval t;
        gettimeofday(&t,NULL);
        if (expire_time_s == 0 || t.tv_sec <expire_time_s){
            std::cout << "Adding " << key << " -> " << value << std::endl;
            m_mapKeyValues[key] = value;
            if (expire_time_ms != 0)
			{
				t.tv_sec = expire_time_ms / 1000;
				t.tv_usec = (expire_time_ms % 1000) * 1000;
				m_mapKeyTimeouts[key] = t;
			}
		}
    }
    rdb.close();
}

std::unique_ptr<std::vector<std::string>> getAllKeys(const std::string& regex)
{
    auto result{std::make_unique<std::vector<std::string>>()};

    std::cout << "Got regex: " << regex << std::endl;
	for (const auto& key: m_mapKeyValues)
	{
		result->push_back(key.first);
	}
	return result;
}



std::unordered_map<std::string, std::string> dictionary = {};
std::unordered_map<std::string,long> expTime;
void handle_connection(int client) {
  std::cout << "Client connected" << client << "\n";
  char buffer[BUFFER_SIZE];
  int bytes_read;
  const char* pong_response = "+PONG\r\n";
  initializeKeyValues();
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
        timeval t;
        gettimeofday(&t,NULL);
        std::cout<<(t.tv_sec)<<" "<<m_mapKeyTimeouts[tokens[4]].tv_sec<<std::endl;
      if (m_mapKeyValues.find(tokens[4])!=m_mapKeyValues.end() && (m_mapKeyTimeouts[tokens[4]].tv_sec == 0 || m_mapKeyTimeouts[tokens[4]].tv_sec > (t.tv_sec))){
        std::string g_response = "$" + std::to_string(m_mapKeyValues[tokens[4]].size()) + "\r\n" + m_mapKeyValues[tokens[4]] + "\r\n";
        send(client, g_response.data(), g_response.length(), 0);
      }
      else if (dictionary.find(tokens[4]) == dictionary.end()){
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
    } else if (cmd == "config"){
        std::string response="*2\r\n";
        if (tokens[4] == "GET"){
            if (tokens[6]=="dir"){
                response+="$3\r\ndir\r\n";
                response+="$" + std::to_string(dir.size()) + "\r\n" + dir + "\r\n";
            }
            else if (tokens[6]=="dbfilename"){
                response += "$10\r\ndbfilename\r\n";
                response += "$" + std::to_string(dbfilename.size()) + "\r\n" + dbfilename + "\r\n";
            }
            send(client,response.data(),response.length(),0);
        }
    } else if (cmd == "keys"){
        
        auto ptr = getAllKeys(tokens[4]);
        std::vector<std::string>* rgx = ptr.get();
        std::string response="*"+std::to_string(rgx->size())+"\r\n";
        
        for (const auto& m : *rgx){
            response+="$"+std::to_string(m.length())+"\r\n"+m+"\r\n";
            std::cout<<response<<std::endl;
            
        }
        send(client,response.data(),response.length(),0);
    }
  }
  close(client);
}
int main(int argc, char **argv) {

    for (int i=0;i<argc;i++){
        if (strcmp(argv[i],"--dir")==0){
            dir = argv[++i];
            continue;
        }
        if (strcmp(argv[i],"--dbfilename")==0){
            dbfilename = argv[++i];
        }
        
    }
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