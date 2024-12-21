#include "Config.h"
#include "Parser.h"
#include <optional>
#include <iostream>
#include <vector>
namespace commands {

std::optional<std::string> Config::inner_handle(const std::span<const std::string>& params, Session* session) {
  
  std::cout<<"path: "<<data_->path<<std::endl;
  std::vector<std::string> res;
  res.push_back("dir");
  res.push_back(data_->path);
  
  auto parsed_message = Parser::encodeRespArray(res);
  return parsed_message;
}

} // namespace commands