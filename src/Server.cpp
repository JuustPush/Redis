#include "Server.h"
#include "Helpers.h"
#include "Parser.h"
#include "Session.h"
#include "Replica.hpp"
#include <asio/completion_condition.hpp>
#include <asio/connect.hpp>
#include <asio/error_code.hpp>
#include <asio/io_context.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <fstream>



Server::Server(asio::io_context &io_context, const ServerConfig &config)
    : acceptor_(io_context, tcp::endpoint(tcp::v4(), config.port)),
      io_context(io_context), data_(std::make_shared<KVStorage>()),
      replication_info_(init_replication_info(config)),
      replica_manager_(std::make_shared<ReplicaManager>())
{
  if (!replication_info_->is_master && !master_handshake(config))
  {
    throw std::runtime_error("Can't connect to master");
  }
  start_accept(); 

  initializeKeyValues(config);
  data_->dbf=config.dbfilename;
  data_->path=config.dir_path;
  
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

int64_t get_current_timestamp()
{
  return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
void Server::initializeKeyValues(const ServerConfig &config){
  if (config.dir_path.empty() || config.dbfilename.empty()) return;
  std::string rdbFullPath = config.dir_path + "/" + config.dbfilename;
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
        std::cout<<"cur time ms: "<<get_current_timestamp()<<std::endl;
        rdb.read(reinterpret_cast<char*>(&opcode), 1);
        }
        
        std::string key = read_byte_to_string(rdb);
        std::string value = read_byte_to_string(rdb);
        
        std::chrono::_V2::system_clock::time_point expiry_time;
        // if (expire_time_ms > 0) expiry_time = std::chrono::system_clock::from_time_t((expire_time_ms/1000)); //std::chrono::milliseconds(expire_time_ms) 
        // else expiry_time = std::chrono::time_point<std::chrono::system_clock>::max();
        //kv[key]=value;
        std::cout<<"key , value : ";
        std::cout<<key<<" "<<value<<" "<<expire_time_ms<<std::endl;
        if (get_current_timestamp()<expire_time_ms || expire_time_ms == 0)
          data_->set(std::move(key), std::move(value));
        //if (expire_time_ms > 0) valid_until_ts[key]=expire_time_ms;
        //kVars.emplace(std::string(key), Value{std::string(value), expiry_time});
        
    }
    rdb.close();
    }

void Server::start_accept()
{
  auto new_session = std::make_shared<Session>(io_context, this);
  acceptor_.async_accept(new_session->get_socket(),
                         [this, new_session](const auto &error)
                         {
                           handle_accept(new_session, error);
                         });
}


void Server::handle_accept(std::shared_ptr<Session> session,
                           const asio::error_code &error_code)
{
  if (!error_code)
  {
    std::cout << "Connected\n";
    
    client_id=generate_client_id();
    
    //forCommander=client_id;
    cli_id=client_id;
    session->start(client_id);
  //asio::ip::tcp::socket socket;
  // auto& socket = session->get_socket(); 
  //   auto endpoint = socket.remote_endpoint();
  //   std::string client_addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
  //   std::cout<<"client addr "<<client_addr<<std::endl;

    
  }
  else
  {
    std::cout << "Error when accept = " << error_code.message() << "\n";
  }
  start_accept();
}


std::shared_ptr<ReplicationInfo>
Server::init_replication_info(const ServerConfig &config)
{
  static constexpr int kReplidLen = 40;
  auto info = std::make_shared<ReplicationInfo>();
  if (config.replicaof.has_value())
  {
    info->is_master = false;
  }
  else
  {
    info->is_master = true;
  }
  info->master_repl_offset = 0;
  info->master_replid = random_string(kReplidLen);
  return info;
}
bool Server::master_handshake(const ServerConfig &config)
{
  master_session_ = std::make_shared<Session>(io_context, this, true);
  auto &socket = master_session_->get_socket();
  tcp::resolver resolver(io_context);
  auto endpoint = resolver.resolve(config.replicaof->host,
                                   std::to_string(config.replicaof->port));

  asio::error_code ec;
  asio::connect(socket, endpoint, ec);

  if (!ec)
  {
    // step1: send PING to master
    // step2: send REPLCONF listening-port <port>
    // step3: send REPLCONF capa sync2
    // step4: send PSYNC ? -1
    std::vector<std::vector<std::string>> commands = {
        {"PING"},
        {"REPLCONF", "listening-port", std::to_string(config.port)},
        {"REPLCONF", "capa", "sync2"},
        {"PSYNC", "?", "-1"}};
    asio::streambuf resp_buffer;
    for (const auto &command : commands)
    {
      std::string message = Parser::encodeRespArray(command);
      asio::error_code command_ec;
      asio::write(socket, asio::buffer(message), command_ec);
      if (command_ec)
      {
        std::cout << "Failed to send command to master, error = "
                  << command_ec.message() << "\n";
        return false;
      }
      int bytes = asio::read_until(socket, resp_buffer, "\r\n");
      std::istream response_stream(&resp_buffer);
      std::string response{
          asio::buffers_begin(resp_buffer.data()),
          asio::buffers_begin(resp_buffer.data()) + bytes - 2};
      resp_buffer.consume(bytes);
      std::cout << "Received: " << response << std::endl;
    }
    int num_bytes = 0;
    int bytes = asio::read_until(socket, resp_buffer, "\r\n");
    std::istream response_stream(&resp_buffer);
    std::string response{
        asio::buffers_begin(resp_buffer.data()),
        asio::buffers_begin(resp_buffer.data()) + bytes - 2};
    resp_buffer.consume(bytes);
    assert(response[0] == '$');
    for (int i = 1; i < response.size(); i++)
    {
      num_bytes = num_bytes * 10 + (response[i] - '0');
    }
    if (resp_buffer.size() > bytes)
    {
      std::string rdb_content = {
          asio::buffers_begin(resp_buffer.data()),
          asio::buffers_begin(resp_buffer.data()) + num_bytes};
      resp_buffer.consume(num_bytes);
      std::string command{asio::buffers_begin(resp_buffer.data()),
                          asio::buffers_end(resp_buffer.data())};
      master_session_->start(client_id);
      master_session_->handle_message(command);
    }
  }
  else
  {
    std::cout << "Failed to connect to master server\n";
    return false;
  }
  return true;
}

std::optional<int> cli_id = std::nullopt;