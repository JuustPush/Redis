#include "Command.h"
#include "Storage.h"
#include <asio/steady_timer.hpp>
#include <iostream>
#include <memory>
#include <optional>
#include <string>

class Replica;

namespace commands {
struct Wait : public Command {
  Wait() : Command(false) {}

  ~Wait() { std::cout << "Destructed wait"; }

  void handle(const std::vector<std::string> &command_list,
              Session *session) override;

  std::optional<std::string>
  inner_handle(const std::span<const std::string> &params,
               Session *session) override;

  void handle_wait(std::shared_ptr<Replica> replica,
                   std::shared_ptr<int> needed, std::shared_ptr<int> acks,
                   int64_t deadline, Session *client_session,
                   std::shared_ptr<asio::steady_timer> timer,
                   std::vector<std::shared_ptr<asio::steady_timer>> timers);
};
} // namespace commands