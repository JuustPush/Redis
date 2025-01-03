#pragma once

#include <cstdint>
#include <unordered_map>
#include <string>
#include <optional>
#include <vector>

class KVStorage {
public:
    void set(std::string k, std::string v, uint32_t expiring_time_ms = -1);

    std::optional<std::string> get(const std::string& k) const;
    
    std::optional<std::string> incr(const std::string &k) const;

    std::vector<std::string> keys() const;

    std::string path;
    std::string dbf;
private:

    struct ExpiringValue {
        std::string value;
        uint64_t expired_ts_ms;
    };

    mutable std::unordered_map<std::string, ExpiringValue> data_;
};