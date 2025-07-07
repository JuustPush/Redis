#include "CommandHandler.h"
#include "Parser.h"
#include "ReplicationInfo.h"
#include "Session.h"
#include "Storage.h"
#include "Server.h"
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
#include <map>
#include <unordered_set>


void def_call_back(const asio::error_code &error_code,
                                size_t len) {
  if (error_code) {
    std::cout << "error = " << error_code.message() << "\n";
  }
}

// bool isInteger(const std::string& str) {
//     try {
//         std::stoi(str); // или std::stol, std::stoll
//         return true;
//     } catch (const std::invalid_argument& e) {
//         return false;
//     } catch (const std::out_of_range& e) {
//         return false;
//     }
// }

bool complex=false;
bool multi = false;
std::map<std::string,std::vector<std::vector<std::string>>> q;
std::unique_ptr<commands::Command>::pointer command;
std::string fd_multi;
std::unordered_set<std::string> streamKeys;

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

void CommandHandler::handle_raw_command(const std::string &raw_command,int client_id){ 
  auto commands = Parser::decode(raw_command);
  for (const auto &command_list : commands) {
    std::string main_command = command_list[0];
    std::transform(main_command.begin(), main_command.end(),
                   main_command.begin(),
                   [](const auto c) { return tolower(c); });

    auto it = command_map_.find(main_command);
    std::cout<<"main command: "<<main_command<<std::endl;

    auto& socket = session_->get_socket(); 
    auto endpoint = socket.remote_endpoint();
    std::string client_addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    std::cout<<"client addr "<<client_addr<<std::endl;



    
    std::cout<<"multi check? "<<multi<<std::endl;
    
    if (it != command_map_.end()) command = it->second.get();

    std::cout << "Can go here first\n";
    

    if (main_command == "type"){
      std::cout<<"im here in type"<<std::endl;
      auto tmp = data_->get(command_list[1]);
      if (tmp.has_value()){
        session_->write("+string\r\n", def_call_back);
      }
      else{
        session_->write("+none\r\n", def_call_back);
      }
      return;
    } else if (main_command == "xadd") {
          std::string stream_key = command_list[1];
          if (streamKeys.find(stream_key) == streamKeys.end()) {
            streamKeys.insert(stream_key);
          }
          if (command_list.size() >= 3) {
            std::string entry_id = command_list[2];
            std::string response = "$" + std::to_string(entry_id.size()) + "\r\n" +
                       entry_id + "\r\n";
            session_->write(response, def_call_back);
          }
          return;
    }
    else if (main_command=="multi"){
      multi=true;
      fd_multi =client_addr;
      std::cout<<"Start fd_multi: "<<fd_multi<<std::endl;
      session_->write("+OK\r\n", def_call_back);
      continue;
    }
    else if (main_command == "exec"){
      
      std::cout<<"q empty? "<<q.empty()<<std::endl;
      std::cout<<"multi? "<<multi<<std::endl;
      if (q[client_addr].empty() && multi == false){
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
    else if (main_command == "discard"){
      if (multi){
        session_->write("+OK\r\n", def_call_back);
        multi=false;
        q.clear();
      }
      else{
        session_->write("-ERR DISCARD without MULTI\r\n", def_call_back);
      }
      return;
    }
    else if (multi){
      if (command_list[0]=="GET"){
        bool flag=false;

        std::cout<<"bool flag? :"<<flag<<std::endl;
        std::string fd_cur=client_addr;
        std::cout<<"fd cur: "<<fd_cur<<std::endl;
        if (fd_cur!=fd_multi) session_->write("$-1\r\n", def_call_back);
        else{
          q[client_addr].push_back(command_list);
          session_->write("+QUEUED\r\n", def_call_back);
        }
      }
      else {
        q[client_addr].push_back(command_list);
        session_->write("+QUEUED\r\n", def_call_back);
        std::cout<<command_list[0]<<" "<<command_list[1]<<std::endl;
      }
    }
    if (!multi && q[client_addr].empty()){
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
      

      std::unordered_map<std::string,std::string> str;
      std::string result;
      result="*"+std::to_string(q[client_addr].size())+"\r\n";
      for (const auto &cl : q[client_addr]){
          std::string mc = cl[0];
          std::cout<<"mc command: "<<mc<<std::endl;
          std::transform(mc.begin(), mc.end(),
                  mc.begin(),
                  [](const auto c) { return tolower(c); });

          if (mc == "set"){
            str[cl[1]]=std::stoi(cl[2]);
            data_->set(std::move(cl[1]), std::move(cl[2]));
            result+="$2\r\nOK\r\n";

          } else if (mc == "get") {
            //"$" + std::to_string(str.size()) + "\r\n" + str + "\r\n";
            auto tmp = data_->get(cl[1]);
            
            result+="$" + std::to_string(tmp.value().size())+"\r\n"+tmp.value() + "\r\n";
          } else if(mc=="incr"){

            std::cout<<"data incr is: "<<cl[1]<<std::endl;
            auto temp = data_->get(cl[1]);
            if (temp.has_value())
            {
              std::cout<<"str cl 1 is: "<<temp.value()<<std::endl;
              if (isInteger(temp.value()))
                {
                  int tmp = std::stoi(temp.value());
                  tmp++;
                  
                  data_->incr(cl[1]);
                  result+=":"+std::to_string(tmp)+"\r\n";
                }
                else{
                  result+="-ERR value is not an integer or out of range\r\n";
                }
              
            } else{
              str[cl[1]]=1;
              data_->set(std::move(cl[1]), std::move("1"));
              result+=":1\r\n";
            }


          }
      }
      std::cout<<"result is: "<<result<<std::endl;
      multi=false;
      q[client_addr].clear();
      session_->write(result, def_call_back);
    }

    if (session_->is_master_session()) {
      replication_info_->updateOffset(
          Parser::encodeRespArray(command_list).size());
    }
  }
}