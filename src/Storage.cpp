#include "Storage.h"
#include "Helpers.h"
#include <ctime>
#include <limits>
#include <optional>


std::optional<std::string> KVStorage::get(const std::string &k) const {
  auto current_time_ms = get_current_timestamp_ms();
  auto it = data_.find(k);
  if (it == data_.end()) {
    return std::nullopt;
  }
  if (it->second.expired_ts_ms <= current_time_ms) {
    data_.erase(k);
    return std::nullopt;
  }
  return it->second.value;
}

std::optional<std::string> KVStorage::incr(const std::string &k) const {
  auto it = data_.find(k);

  if (it == data_.end()) {
    return std::nullopt;
  }
  int val = std::stoi(it->second.value);
  val++;
  it->second.value=std::to_string(val);
  return it->second.value;
}

std::vector<std::string> KVStorage::keys() const {
  std::vector<std::string> res;
  for (auto &d : data_){
    res.push_back(d.first);
  }
  return res;
}

void KVStorage::set(std::string k, std::string v, uint32_t expiring_time_ms) {
  uint64_t expired_ts_ms =
      expiring_time_ms == -1
          ? std::numeric_limits<uint64_t>::max()
          : get_current_timestamp_ms() + expiring_time_ms;
  data_.insert_or_assign(std::move(k),
                         ExpiringValue{std::move(v), expired_ts_ms});
}