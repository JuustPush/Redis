#include "ReplicationInfo.h"

std::string ReplicationInfo::returnRawInfo() const {
  return "role:" + (is_master ? std::string("master") : std::string("slave")) + "\n"
         "master_replid:" + master_replid + "\n"
         "master_repl_offset:" + std::to_string(master_repl_offset);
}

void ReplicationInfo::updateOffset(int delta) {
  offset += delta;
}

int ReplicationInfo::getOffset() {
  return offset;
}