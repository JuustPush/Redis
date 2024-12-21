#include "Psync.h"
#include "Parser.h"
#include "Session.h"

namespace commands {

std::optional<std::string>
Psync::inner_handle(const std::span<const std::string> &params, Session *session) {
  std::string response = "FULLRESYNC " + rep_info_->master_replid + " 0";
  response = Parser::encodeString(response);
  return response;
}

void Psync::after_write(const std::span<const std::string> &params,
                        Session *session) {
  std::string rdb =
      "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76"
      "\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69"
      "\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08"
      "\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66"
      "\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
  std::string rdb_return = "$" + std::to_string(rdb.size()) + "\r\n" + rdb;
  session->write(rdb_return, default_call_back, false);
  session->set_as_replica();
}
} // namespace commands