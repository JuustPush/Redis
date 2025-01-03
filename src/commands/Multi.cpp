#include "Multi.h"
#include "Parser.h"
#include <optional>

namespace commands {

std::optional<std::string> Multi::inner_handle(const std::span<const std::string>& params, Session* session) {
  

  
  
  return Parser::encodeString("OK");
}


}