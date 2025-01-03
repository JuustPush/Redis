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
#include <algorithm>
#include <iostream>
#include <memory>

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
    if (it == command_map_.end()) {
      std::cout << "Incorrect command, command = " << main_command << "\n";
      return;
    }

    std::cout << "Can go here\n";
    auto command = it->second.get();

  
    command->handle(command_list, session_);

    if (session_->is_master_session()) {
      replication_info_->updateOffset(
          Parser::encodeRespArray(command_list).size());
    }
  }
}