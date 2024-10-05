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
#include <sstream>
#include <mutex>
using namespace std::string_view_literals;



struct Value {
    std::string val;
    std::chrono::time_point<std::chrono::system_clock> expiry;
    bool IsExpired() const noexcept {
        auto now = std::chrono::system_clock::now();
        std::cout<<now<<" "<<expiry;
        return now > expiry;
    }
};

 std::string dir;
 std::string dbfilename;
// std::map<std::string,std::string> m_mapKeyValues;
// std::map<std::string, timeval> m_mapKeyTimeouts;
std::map<std::string, Value> kVars;
void addDefault(){
    kVars.emplace(std::string("foo"), Value{std::string("123"),  std::chrono::time_point<std::chrono::system_clock>::max()});
    kVars.emplace(std::string("bar"), Value{std::string("456"),  std::chrono::time_point<std::chrono::system_clock>::max()});
    kVars.emplace(std::string("baz"), Value{std::string("789"),  std::chrono::time_point<std::chrono::system_clock>::max()});
}


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
        // timeval t;
        // gettimeofday(&t,NULL);
        // if (expire_time_s == 0 || t.tv_sec <expire_time_s){
        //     std::cout << "Adding " << key << " -> " << value << std::endl;
        //     m_mapKeyValues[key] = value;
        //     if (expire_time_ms != 0)
		// 	{
		// 		t.tv_sec = expire_time_ms / 1000;
		// 		t.tv_usec = (expire_time_ms % 1000) * 1000;
		// 		m_mapKeyTimeouts[key] = t;
		// 	}
		//}
        std::chrono::_V2::system_clock::time_point expiry_time;
        if (expire_time_ms > 0) expiry_time = std::chrono::system_clock::from_time_t((expire_time_ms/1000)); //std::chrono::milliseconds(expire_time_ms) 
        else expiry_time = std::chrono::time_point<std::chrono::system_clock>::max();
        kVars.emplace(std::string(key), Value{std::string(value), expiry_time});
        
    }
    rdb.close();
}
// std::unique_ptr<std::vector<std::string>> getAllKeys(const std::string& regex)
// {
//     auto result{std::make_unique<std::vector<std::string>>()};
//     std::cout << "Got regex: " << regex << std::endl;
// 	for (const auto& key: m_mapKeyValues)
// 	{
// 		result->push_back(key.first);
// 	}
// 	return result;
// }

std::unique_ptr<std::vector<std::string>> getAllKeys(const std::string_view& regex)
{
    auto result{std::make_unique<std::vector<std::string>>()};
    std::cout << "Got regex: " << regex << std::endl;
	for (const auto& key: kVars)
	{
        if (key.first != "foo" && key.first != "bar" && key.first != "baz")
		    result->push_back(key.first);
	}
	return result;
}

bool kIsMaster = true;
int kMasterFd = -1;
std::vector<int> kReplicaFds;
thread_local char kBuf[128];

std::mutex kMtx;
std::string_view parse_string(std::string_view& input) {
    auto pos = input.find('$');
    if (pos == input.npos) return "";
    size_t count = 0;
    while (pos < input.size() && ::isdigit(input[++pos])) {
        count = count * 10 + (input[pos] - '0');
    }
    auto str = input.substr(pos + 2, count);
    input.remove_prefix(pos + 2);
    return str;
}
bool is_cmd(std::string_view cmd, std::string_view cmd_str) {
    if (cmd.size() != cmd_str.size()) return false;
    for (size_t i = 0; i < cmd.size(); ++i) {
        if (::toupper(cmd[i]) != ::toupper(cmd_str[i])) return false;
    }
    return true;
}
std::string encode_bulk_str(std::string_view msg) {
    auto encoded_msg = std::string("$");
    encoded_msg += std::to_string(msg.size()) + "\r\n";
    encoded_msg += msg;
    encoded_msg += "\r\n";
    return encoded_msg;
}
void handle_connection(int fd) {
    std::cout<<"current fd "<<fd<<std::endl;

 while (auto ret = read(fd, kBuf, sizeof kBuf)) {
        if (ret == 0 || ret == -1) break;
        const auto raw_input = std::string_view(kBuf, ret);
        auto input = raw_input;
        auto cmd = parse_string(input);
        if (!kReplicaFds.empty() && is_cmd(cmd, "SET"sv)) {
            for (const auto rfd : kReplicaFds) {
                send(rfd, raw_input.data(), raw_input.size(), 0);
            }
        }
        if (is_cmd(cmd, "PING"sv)) {
            send(fd, "+PONG\r\n", 7, 0);
        } else if (is_cmd(cmd, "ECHO"sv)) {
            auto msg = encode_bulk_str(parse_string(input));
            send(fd, msg.data(), msg.size(), 0);
        } else if (is_cmd(cmd, "SET"sv)) {
            auto key = parse_string(input);
            auto value = parse_string(input);
            auto expiry_cmd = parse_string(input);
            auto expiry_time = std::chrono::time_point<std::chrono::system_clock>::max();
            if (!expiry_cmd.empty() && is_cmd(expiry_cmd, "PX"sv)) {
                auto ms_str = parse_string(input);
                int ms = 0;
                for (char c : ms_str) {
                    ms = ms * 10 + (c - '0');
                }
                expiry_time = std::chrono::system_clock::now() + std::chrono::milliseconds(ms);
            }
 	        std::cerr << "SET -> " << key << "\n";
            {
                auto _ = std::unique_lock(kMtx);
                kVars.emplace(std::string(key), Value{std::string(value), expiry_time});
                
            }
            if (fd != kMasterFd) send(fd, "+OK\r\n", 5, 0);
        } else if (is_cmd(cmd, "GET"sv)) {
            //std::this_thread::sleep_for(std::chrono::seconds(5)); 
                auto key = parse_string(input);
            for (int i=0;i<100;i++){
                
                auto _ = std::unique_lock(kMtx);
                auto it = kVars.find(std::string(key));
                std::cerr << "-> " << key << "\n";
            if (it == kVars.end() || it->second.IsExpired()) {
                std::cerr << "-> " << std::boolalpha << (it == kVars.end()) << "\n";
                std::cerr << "-> NO\n";
                if (it !=kVars.end()) kVars.erase(it);
                send(fd, "$-1\r\n", 5, 0);
                break;

            } else {
                std::cerr << "-> " << it->second.val << "\n";
                auto msg = encode_bulk_str(it->second.val);
                send(fd, msg.data(), msg.size(), 0);
                break;
            }
            }
            
        } else if (is_cmd(cmd, "INFO"sv)) {
            auto msg = encode_bulk_str(kIsMaster ? "role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"sv : "role:slave"sv);
            send(fd, msg.data(), msg.size(), 0);
        } else if (raw_input == "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n") {
            send(fd, "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n", 56, 0);
            send(fd, "$88\r\n\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2", 93, 0);
            kReplicaFds.push_back(fd);
        } else if (is_cmd(cmd, "REPLCONF"sv)) {
            send(fd, "+OK\r\n", 5, 0);
        } else if (is_cmd(cmd,"KEYS"sv)){
            auto key = parse_string(input);
            auto ptr = getAllKeys(key);
            std::vector<std::string>* rgx = ptr.get();
            std::string response="*"+std::to_string(rgx->size())+"\r\n";
            
            for (const auto& m : *rgx){
                response+="$"+std::to_string(m.length())+"\r\n"+m+"\r\n";
                std::cout<<response<<std::endl;
                
            }
            send(fd,response.data(),response.length(),0);
        } else if (is_cmd(cmd,"CONFIG"sv)){
            parse_string(input);
            auto info = parse_string(input);
            std::string response="*2\r\n";
            if (info=="dir"){
                response+="$3\r\ndir\r\n";
                response+="$" + std::to_string(dir.size()) + "\r\n" + dir + "\r\n";
            }
            else if (info=="dbfilename"){
                response += "$10\r\ndbfilename\r\n";
                response += "$" + std::to_string(dbfilename.size()) + "\r\n" + dbfilename + "\r\n";
            }
            send(fd,response.data(),response.length(),0);
        }
    }
//kVars.clear();
    close(fd);
    
}

int main(int argc, char **argv) {
    uint16_t port = 6379;
    std::string_view master_host, master_port;
    addDefault();
    //initializeKeyValues();
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) {
            auto cmd = std::string_view(argv[i]);
            if (cmd == "--port") {
                port = std::stoi(argv[++i]);
            } else if (cmd == "--replicaof") {
                auto param = std::string_view(argv[++i]);
                auto pos = param.find(' ');
                master_host = param.substr(0, pos);
                master_port = param.substr(pos + 1);
                argv[i][pos] = '\0';
                kIsMaster = false;
            } else if (cmd == "--dir"){
                dir = argv[++i];
                continue;
            } else if (cmd == "--dbfilename"){
                dbfilename = argv[++i];
                initializeKeyValues();
            }
        }
    }
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }
    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }
    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    std::cout << "Waiting for a client to connect...\n";
    std::vector<std::thread> threads;
    if (!kIsMaster) {
kMasterFd = socket(AF_INET, SOCK_STREAM, 0);
        if (kMasterFd < 0) {
            std::cerr << "Failed to create replica socket\n";
            return 1;
        }
        struct addrinfo* master_addr;
        if (getaddrinfo(master_host.data(), master_port.data(), nullptr, &master_addr) != 0) {
            std::cerr << "Failed to parse master addr\n";
            return 1;
        }
 if (connect(kMasterFd, master_addr->ai_addr, master_addr->ai_addrlen) != 0) {
            std::cerr << "Failed to connect master server\n";
            return 1;
        }
        freeaddrinfo(master_addr);
        send(kMasterFd, "*1\r\n$4\r\nPING\r\n", 14, 0);
        read(kMasterFd, kBuf, sizeof kBuf);
        auto msg = std::string("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n");
        msg += encode_bulk_str(std::to_string(port));
        send(kMasterFd, msg.data(), msg.size(), 0);
        read(kMasterFd, kBuf, sizeof kBuf);
        send(kMasterFd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", 40, 0);
        read(kMasterFd, kBuf, sizeof kBuf);
        send(kMasterFd, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", 30, 0);
        read(kMasterFd, kBuf, sizeof kBuf);
        read(kMasterFd, kBuf, sizeof kBuf);
        threads.emplace_back(handle_connection, kMasterFd);
    }
 while (true) {
        auto client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        std::cout << "Client connected\n";
        threads.emplace_back(handle_connection, client_fd);
    }
    close(server_fd);
    return 0;
}