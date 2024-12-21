#include "Info.h"
#include "Parser.h"

namespace commands {

std::optional<std::string> Info::inner_handle(const std::span<const std::string>& params, Session* session) {
  int num_params = params.size();
  if (num_params != 1) {
    return std::nullopt;
  }
  auto info = replication_info_->returnRawInfo();
  return Parser::encodeBulkString(info);
}
} // namespace commands