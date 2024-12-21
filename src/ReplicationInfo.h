#pragma once
#include <cstdint>
#include <string>
#include <atomic>

struct ReplicationInfo {
  bool is_master;
  std::string master_replid;
  uint32_t master_repl_offset;
  std::atomic<int> offset{0};

  std::string returnRawInfo() const;

  void updateOffset(int delta);

  int getOffset();
};