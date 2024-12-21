#include "Command.h"
#include <span>
#include <string>

namespace commands {
struct Echo : public Command {
  Echo() : Command(true) {}

  ~Echo() = default;

  std::optional<std::string> inner_handle(const std::span<const std::string>& params, Session* session) override;
};
} // namespace commands