#include "Set.h"
#include "Parser.h"

namespace commands {

std::optional<std::string>
Set::inner_handle(const std::span<const std::string> &params,
                  Session* session) {
  int num_params = params.size();
  if (num_params < 2) {
    return std::nullopt;
  }
  auto key = params[0];
  auto value = params[1];
  if (num_params == 2) {
    data_->set(std::move(key), std::move(value));
    return Parser::encodeString("OK");
  } else if (num_params == 4 && params[2] == "px") {
    auto expiring_time = std::stoi(params[3]);
    data_->set(std::move(key), std::move(value), expiring_time);
    return Parser::encodeString("OK");
  }
  return std::nullopt;
}

} // namespace commands