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
std::vector<std::vector<std::string>> q;
std::unique_ptr<commands::Command>::pointer command;
int fd_multi;

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
    
    std::cout << "Can go here first\n";
    
    if (it != command_map_.end()) command = it->second.get();
    
    
    
    if (fd_multi) std::cout<<"fd multi: "<<fd_multi<<std::endl;
    if (main_command=="multi"){
      multi=true;
      fd_multi =session_->socket_.native_handle();
      std::cout<<"Start fd_multi: "<<fd_multi<<std::endl;
      session_->write("+OK\r\n", def_call_back);
      continue;
    }
    else if (main_command == "exec"){
      
      std::cout<<"q empty? "<<q.empty()<<std::endl;
      std::cout<<"multi? "<<multi<<std::endl;
      if (q.empty() && multi == false){
        std::cout<<"test queue empty"<<std::endl;
        session_->write("-ERR EXEC without MULTI\r\n", def_call_back);
        multi=false;
        return;
      }
      else if (q.empty()) {
        session_->write("*0\r\n", def_call_back);
        std::cout<<"Im here"<<std::endl;
        multi=false;
        return;
      }
    }

    else if (multi){
      if (command_list[0]=="GET"){
        bool flag=false;
        //std::cout<<"q size: "<<q.size()<<std::endl;
        for (int i=0;i<q.size();i++){
          std::cout<<q[i][1]<<" "<<command_list[1]<<std::endl;
          if (q[i][1] == command_list[1]){
            flag = true;
          }
        } 
        std::cout<<"bool flag? :"<<flag<<std::endl;
        int fd_cur=session_->socket_.native_handle();
        std::cout<<"fd cur: "<<fd_cur<<std::endl;
        if (!flag || fd_cur!=fd_multi) session_->write("$-1\r\n", def_call_back);
        else{
          q.push_back(command_list);
          session_->write("+QUEUED\r\n", def_call_back);
        }
      }
      else {
        q.push_back(command_list);
        session_->write("+QUEUED\r\n", def_call_back);
        std::cout<<command_list[0]<<" "<<command_list[1]<<std::endl;
      }
    }
    if (!multi){
      command->handle(command_list, session_);
    }
    else if (main_command == "exec"){
      std::cout<<"exec execution"<<std::endl;
      std::cout<<"q size: "<<q.size()<<std::endl;
      if (session_ == nullptr) {
      std::cout << "Session is null!" << std::endl;
      return;
      }

      std::string mc = command_list[0];
      std::transform(mc.begin(), mc.end(),
                   mc.begin(),
                   [](const auto c) { return tolower(c); });
      std::cout<<"mc command: "<<mc<<std::endl;

      std::unordered_map<std::string,int> str;
      std::string result="*"+std::to_string(q.size())+"\r\n";
    //   std::string result = "*" + std::to_string(arr.size()) + "\r\n";
    // for (const auto &str : arr)
    // {
    //   result += "$" + std::to_string(str.size()) + "\r\n" + str + "\r\n";
    // }

      for (const auto &cl : q){
          std::string mc = cl[0];
          std::transform(mc.begin(), mc.end(),
                  mc.begin(),
                  [](const auto c) { return tolower(c); });

          // auto it = command_map_.find(mc);
          // if (it != command_map_.end()) command = it->second.get();
          // std::cout<<cl[0]<<" "<<cl[1]<<std::endl;
          // command->handle(cl, session_);
          if (mc == "set"){
            str[cl[1]]=std::stoi(cl[2]);
            data_->set(std::move(cl[1]), std::move(cl[2]));
            result+="$2\r\nOK\r\n";

          } else if (mc == "get") {
            //"$" + std::to_string(str.size()) + "\r\n" + str + "\r\n";
            result+="$" + std::to_string(std::to_string(str[cl[1]]).size())+"\r\n"+std::to_string(str[cl[1]]) + "\r\n";
          } else if(mc=="incr"){
            if (str.find(cl[1])!=str.end())
            {
              str[cl[1]]++;
              data_->incr(cl[1]);
              result+=":"+std::to_string(str[cl[1]])+"\r\n";
            } else{
              str[cl[1]]=1;
              data_->set(std::move(cl[1]), std::move("1"));
              result+=":1\r\n";
            }


          }
      }
      std::cout<<"result is: "<<result<<std::endl;
      multi=false;
      session_->write(result, def_call_back);
    }
    

    if (session_->is_master_session()) {
      replication_info_->updateOffset(
          Parser::encodeRespArray(command_list).size());
    }
  }
}