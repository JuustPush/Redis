#include "Multi.h"
#include "Parser.h"
#include <optional>

namespace commands {

std::optional<std::string> Multi::inner_handle(const std::span<const std::string>& params, Session* session) {
  int num_params = params.size();
  if (num_params < 1) {
    return std::nullopt;
  }

  
  
  return Parser::encodeString("OK");
}


}