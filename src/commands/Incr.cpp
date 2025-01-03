#include "Incr.h"
#include "Parser.h"
#include <optional>

namespace commands {

std::optional<std::string> Incr::inner_handle(const std::span<const std::string>& params, Session* session) {
  int num_params = params.size();
  if (num_params < 1) {
    return std::nullopt;
  }
  const auto &key = params[0];
  auto ret = data_->incr(key);
  if (ret == std::nullopt) {
    return std::nullopt;
  }
  else if (ret.value()=="error") return "-ERR value is not an integer or out of range\r\n";
  return Parser::encodeInt(ret.value());
}

} // namespace commands