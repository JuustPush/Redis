#include "CommandHandler.h"
#include "Parser.h"
#include "ReplicationInfo.h"
#include "Session.h"
#include "Storage.h"
#include "commands/Command.h"
#include "commands/Echo.h"
#include "commands/Get.h"
#include "commands/Info.h"
#include "commands/Ping.h"
#include "commands/Psync.h"
#include "commands/Replconf.h"
#include "commands/Set.h"
#include "commands/Wait.h"
#include "commands/Keys.h"
#include "commands/Config.h"
#include "commands/Incr.h"
#include "commands/Multi.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <queue>
#include <utility>


void def_call_back(const asio::error_code &error_code,
                                size_t len) {
  if (error_code) {
    std::cout << "error = " << error_code.message() << "\n";
  }
}
bool multi = false;

CommandHandler::CommandHandler(
    std::shared_ptr<KVStorage> data,
    std::shared_ptr<ReplicationInfo> replication_info, Session *session)
    : data_(std::move(data)), replication_info_(std::move(replication_info)),
      session_(session) {
  command_map_.emplace("ping", std::make_unique<commands::Ping>());
  command_map_.emplace("echo", std::make_unique<commands::Echo>());
  command_map_.emplace("set", std::make_unique<commands::Set>(data_));
  command_map_.emplace("get", std::make_unique<commands::Get>(data_));
  command_map_.emplace("info",
                       std::make_unique<commands::Info>(replication_info_));
  command_map_.emplace("replconf",
                       std::make_unique<commands::Replconf>(replication_info_));
  command_map_.emplace("psync",
                       std::make_unique<commands::Psync>(replication_info_));
  command_map_.emplace("wait", std::make_unique<commands::Wait>());
  command_map_.emplace("keys", std::make_unique<commands::Keys>(data_));
  command_map_.emplace("config", std::make_unique<commands::Config>(data_));
  command_map_.emplace("incr", std::make_unique<commands::Incr>(data_));
  //command_map_.emplace("multi",std::make_unique<commands::Multi>(data_));
}

void CommandHandler::handle_raw_command(const std::string &raw_command) {
  auto commands = Parser::decode(raw_command);
  for (const auto &command_list : commands) {
    std::string main_command = command_list[0];
    std::transform(main_command.begin(), main_command.end(),
                   main_command.begin(),
                   [](const auto c) { return tolower(c); });

    auto it = command_map_.find(main_command);
    std::cout<<"main command: "<<main_command<<std::endl;
    // if (it == command_map_.end() || (main_command != "multi" && main_command != "exec")) {
    //   std::cout << "Incorrect command, command = " << main_command << "\n";
    //   return;
    // }
    std::queue<std::vector<std::string>> q;
    std::cout << "Can go here first\n";
    std::unique_ptr<commands::Command>::pointer command;
    if (it != command_map_.end()) command = it->second.get();
    
    if (main_command=="multi"){
      multi=true;
      session_->write("+OK\r\n", def_call_back);
      continue;
    }
    else if (main_command == "exec"){
      
      std::cout<<"q empty? "<<q.empty()<<std::endl;
      std::cout<<"multi? "<<multi<<std::endl;
      if (q.empty() && multi == false){
        std::cout<<"test queue empty"<<std::endl;
        session_->write("-ERR EXEC without MULTI\r\n", def_call_back);
      }
      else if (q.empty()) {
        session_->write("*0\r\n", def_call_back);
        std::cout<<"Im here"<<std::endl;
      }
      multi=false;
      return;
    }

    if (multi){
      if (command_list[0]=="GET"){
        session_->write("$-1\r\n", def_call_back);
      }
      else {
        q.push(command_list);
        session_->write("+QUEUED\r\n", def_call_back);
        std::cout<<command_list[0]<<" "<<command_list[1]<<std::endl;
      }
    }
    if (!multi){
      command->handle(command_list, session_);
    }
    

    if (session_->is_master_session()) {
      replication_info_->updateOffset(
          Parser::encodeRespArray(command_list).size());
    }
  }
}