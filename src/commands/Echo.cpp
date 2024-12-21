#include "Echo.h"
#include "Parser.h"

namespace commands {

std::optional<std::string> Echo::inner_handle(const std::span<const std::string>& params, Session* session) {
  int num_params = params.size();
  if (num_params != 1) {
    return std::nullopt;
  }
  auto parsed_message = Parser::encodeString(params[0]);
  return parsed_message;
}

} // namespace commands