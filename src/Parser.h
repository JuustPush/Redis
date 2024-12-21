#pragma once

#include <vector>
#include <string>

class Parser {
public:
    static std::vector<std::vector<std::string>> decode(const std::string& command);

    static std::string encodeString(const std::string& str);

    static std::string encodeBulkString(const std::string& str);

    static std::string encodeRespArray(const std::vector<std::string>);
};