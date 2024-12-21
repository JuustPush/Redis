#include "Ping.h"
#include "Parser.h"

namespace commands {
std::optional<std::string>
Ping::inner_handle(const std::span<const std::string> &params,
                   Session* session) {
  return Parser::encodeString("PONG");
}
} // namespace commands