#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <variant>
#include <mutex>
#include <cstddef>
#include <concepts>
#include <functional>
#include <stdexcept>
#include <csignal>


namespace Redis {
  inline namespace Resp {
    struct RespString {
      // whether the string is simple or bulk
      enum class Type { SimpleString, BulkOrVerbatimString } type;
      std::string value;
      // only used for verbatim strings
      std::string encoding;
    };
    struct RespInteger {
      int64_t value;
    };
    struct RespDouble {
      double value;
    };
    struct RespBoolean {
      bool value;
    };

    struct RespNull {
      static constexpr std::nullptr_t value = nullptr;
    };
    struct RespBigInteger {
      std::string value;
    };
    struct RespError {
      // whether the error is simple or bulk
      enum class Type { SimpleError, BulkError } type;
      std::string value;
    };

    struct RespArray;
    struct RespMap;
    struct RespSet;
    struct RespPush;

    using RespValue = std::variant<RespString, RespError, RespInteger, RespBigInteger, RespDouble, RespBoolean, RespNull,
                                RespArray, RespMap, RespSet, RespPush>;

    struct RespValueComparator {
    bool operator()(const RespValue& lhs, const RespValue& rhs) const;
    };
    struct RespArray {
        std::vector<RespValue> value;
    };
    struct RespMap {
        std::vector<std::pair<RespValue, RespValue>> value;
    };
    struct RespSet {
        std::vector<RespValue> value;
    };
    struct RespPush {
        std::vector<RespValue> value;
    };

  }


  struct IOError : public std::runtime_error {
    using std::runtime_error::runtime_error;
  };

  struct EndOfStream : public IOError {
    using IOError::IOError;
  };

  struct InvalidRespException : public std::runtime_error {
      using std::runtime_error::runtime_error;
  };

  template <typename T>
  concept RawReader = requires(T t, char* buffer, size_t size) {
      { t(buffer, size) } -> std::same_as<ssize_t>;
  };

  template <typename T>
  concept RawWriter = requires(T t, const char* buffer, size_t size) {
      { t(buffer, size) } -> std::same_as<ssize_t>;
  };

  template <size_t N> struct CharBuffer{
    static constexpr size_t capacity = N;
    std::array<char, N> bytes{};
    size_t buffer_first{},buffer_last{};
    auto size() const { return buffer_last - buffer_first; }
    bool empty() const { return buffer_first == buffer_last; }
    void clear() { buffer_first = buffer_last = 0; }
    void consume(size_t n) { buffer_first +=n; }
    void extend(size_t n) { buffer_last +=n; }
  };

  template <RawReader R> struct BufferedReader {
    R reader;
    CharBuffer<1024> buffer{};

        RespString parseSimpleString() {
        return RespString{
            .type = RespString::Type::SimpleString,
            .value = nextSimpleEncodedChars(),
        };
    }
    RespError parseSimpleError() {
        return RespError{
            .type = RespError::Type::SimpleError,
            .value = nextSimpleEncodedChars(),
        };
    }
    RespInteger parseInteger() {
        return RespInteger{
            .value = nextInteger(),
        };
    }
    RespString parseBulkString() {
        return RespString{
            .type = RespString::Type::BulkOrVerbatimString,
            .value = nextBulkEncodedChars(),
        };
    }
    RespArray parseArray() {
        int64_t length = nextInteger();
        if (length < 0) {
            throw InvalidRespException("Invalid RESP array");
        }
        RespArray array{};
        array.value.reserve(length);
        for (int64_t i = 0; i < length; i++) {
            array.value.emplace_back(readValue());
        }
        return array;
    }
    RespNull parseNull() {
        if (nextChar() != '\r' || nextChar() != '\n') {
            throw InvalidRespException("Invalid RESP null");
        }
        return RespNull{};
    }
    RespBoolean parseBoolean() {
        char c = nextChar();
        bool value;
        if (c == 't') {
            value = true;
        }
        else if (c == 'f') {
            value = false;
        }
        else {
            throw InvalidRespException("Invalid RESP boolean");
        }
        if (nextChar() != '\r' || nextChar() != '\n') {
            throw InvalidRespException("Invalid RESP boolean");
        }
        return RespBoolean{
            .value = value,
        };
    }
    RespDouble parseDouble() {
        std::string chars = nextSimpleEncodedChars();
        try {
            return RespDouble{
                .value = std::stod(chars),
            };
        }
        catch (const std::logic_error& e) {
            throw InvalidRespException("Invalid RESP double");
        }
    }
    RespBigInteger parseBigInteger() {
        return RespBigInteger{
            .value = nextSimpleEncodedChars(),
        };
    }
    RespError parseBulkError() {
        return RespError{
            .type = RespError::Type::BulkError,
            .value = nextBulkEncodedChars(),
        };
    }
    RespString parseVerbatismString() {
        int64_t length = nextInteger();
        if (length < 4) {
            throw InvalidRespException("Invalid RESP verbatim string");
        }
        std::string encoding(3, 0);
        nextChars(encoding.data(), 3);
        if (nextChar() != ':') {
            throw InvalidRespException("Invalid RESP verbatim string");
        }
        std::string chars(length - 4, 0);
        nextChars(chars.data(), length - 4);
        if (nextChar() != '\r' || nextChar() != '\n') {
            throw InvalidRespException("Invalid RESP verbatim string");
        }
        return RespString{
            .type = RespString::Type::BulkOrVerbatimString,
            .value = std::move(chars),
            .encoding = std::move(encoding),
        };
    }
    RespMap parseMap() {
        int64_t length = nextInteger();
        if (length < 0) {
            throw InvalidRespException("Invalid RESP map");
        }
        RespMap map{};
        for (int64_t i = 0; i < length; i++) {
            RespValue key = readValue();
            RespValue value = readValue();
            map.value.emplace_back(std::move(key), std::move(value));
        }
        return map;
    }
    RespSet parseSet() {
        int64_t length = nextInteger();
        if (length < 0) {
            throw InvalidRespException("Invalid RESP set");
        }
        RespSet set{};
        set.value.reserve(length);
        for (int64_t i = 0; i < length; i++) {
            set.value.emplace_back(readValue());
        }
        return set;
    }
    RespPush parsePush() {
        int64_t length = nextInteger();
        if (length < 0) {
            throw InvalidRespException("Invalid RESP push");
        }
        RespPush push{};
        push.value.reserve(length);
        for (int64_t i = 0; i < length; i++) {
            push.value.emplace_back(readValue());
        }
        return push;
    }



    RespValue readValue(){
      char type = nextChar();
      switch (type) {
      case '+':
          return parseSimpleString();
      case '-':
          return parseSimpleError();
      case ':':
          return parseInteger();
      case '$':
          return parseBulkString();
      case '*':
          if (peekChar() == '-') {
              nextInteger();
              return RespNull{};
          }
          return parseArray();
      case '_':
          return parseNull();
      case '#':
          return parseBoolean();
      case ',':
          return parseDouble();
      case '(':
          return parseBigInteger();
      case '!':
          return parseBulkError();
      case '%':
          return parseMap();
      case '~':
          return parseSet();
      case '>':
          return parsePush();
      default:
          throw InvalidRespException("Invalid RESP object type");
      }
      // unreachable
      __builtin_unreachable();
    }
    private:
    
    int64_t nextInteger() {
    int64_t value{};
    char c;
    while ((c = nextChar()) != '\r') {
        value = value * 10 + (c - '0');
    }
    if (nextChar() != '\n') {
        throw InvalidRespException("Invalid RESP integer");
    }
    return value;
    }
    std::string nextSimpleEncodedChars() {
        std::string value;
        char c;
        while ((c = nextChar()) != '\r') {
            value.push_back(c);
        }
        if (nextChar() != '\n') {
            throw InvalidRespException("Invalid RESP simple string");
        }
        return value;
    }
    std::string nextBulkEncodedChars() {
        int64_t length = nextInteger();
        if (length < 0) {
            throw InvalidRespException("Invalid RESP bulk string");
        }
        std::string value;
        value.resize(length);
        nextChars(value.data(), length);
        if (nextChar() != '\r' || nextChar() != '\n') {
            throw InvalidRespException("Invalid RESP bulk string");
        }
        return value;
    }
    char nextChar() {
        if (buffer.empty()) {
            readMore();
        }
        return buffer.bytes[buffer.buffer_first++];
    }
    void nextChars(char* out, size_t n) {
        size_t off{};
        while (off < n) {
            if (buffer.empty()) {
                readMore();
            }
            size_t to_copy = std::min(n - off, buffer.size());
            std::copy_n(buffer.bytes.data() + buffer.buffer_first, to_copy, out + off);
            buffer.consume(to_copy);
            off += to_copy;
        }
    }
    char peekChar() {
        if (buffer.empty()) {
            readMore();
        }
        return buffer.bytes[buffer.buffer_first];
    }
    void readMore() {
        ssize_t bytes_read = reader(buffer.bytes.data() + buffer.buffer_last, buffer.capacity - buffer.buffer_last);
        if (bytes_read < 0) {
            throw IOError("Failed to read more bytes");
        }
        else if (bytes_read == 0) {
            throw EndOfStream("End of stream reached");
        }
        buffer.buffer_last += bytes_read;
    }

  };
  
  template <RawWriter W> struct BufferedWriter {
    W writer;
    CharBuffer<1024> buffer{};

    void flush(){
      while(buffer.size() > 0) {
        ssize_t bytes_written = writer(buffer.bytes.data() + buffer.buffer_first,buffer.size());
        if (bytes_written < 0){
          throw IOError("Failed to write bytes");
        }
        else if (bytes_written == 0){
          throw EndOfStream("End of stream reached");
        }
        buffer.consume(bytes_written);
      }
    }

    void print(std::string_view bytes){
      size_t off{};
      while(off<bytes.size()){
        if (buffer.size() == buffer.capacity){
          flush();
        }
        size_t to_copy = std::min(bytes.size() - off, buffer.capacity - buffer.buffer_last);
        std::copy_n(bytes.data()+off,to_copy,buffer.bytes.data()+buffer.buffer_last);
        buffer.extend(to_copy);
        off+=to_copy;
      }
    }

    void print(std::integral auto value) { print(std::to_string(value)); }
    void print(std::floating_point auto value) { print(std::to_string(value)); }
    template <typename T1, typename T2, typename... Args> void print(T1&& t1,T2&& t2, Args &&...args){
      print(std::forward<T1>(t1));
      print(std::forward<T2>(t2),std::forward<Args>(args)...);
    }


    void writeValue(const RespString& value){
      using namespace std::string_view_literals;
      if (value.type == RespString::Type::SimpleString) {
        print("+"sv,value.value,"\r\n"sv);
      }
      else{
        if (value.encoding.empty()){
          print("$"sv,value.value.size(),"\r\n"sv,value.value,"\r\n"sv);
        }
        else{
          print("="sv,value.value.size()+4,value.encoding, ":"sv,value.value,"\r\n"sv);

        }
      }
    }
    
    void writeValue(const RespError& value) {
    using namespace std::string_view_literals;
    if (value.type == RespError::Type::SimpleError) {
        print("-"sv, value.value, "\r\n"sv);
    }
    else {
        print("!"sv, value.value.size(), "\r\n"sv, value.value, "\r\n"sv);
    }
}
    void writeValue(const RespInteger& value) {
        using namespace std::string_view_literals;
        print(":"sv, value.value, "\r\n"sv);
    }
    void writeValue(const RespArray& value) {
        using namespace std::string_view_literals;
        print("*"sv, value.value.size(), "\r\n"sv);
        for (const auto& v : value.value) {
            writeValue(v);
        }
    }
    void writeValue(const RespNull& value) {
        using namespace std::string_view_literals;
        print("_\r\n"sv);
    }
    void writeValue(const RespBoolean& value) {
        using namespace std::string_view_literals;
        print("#"sv, value.value ? "t"sv : "f"sv, "\r\n"sv);
    }
    void writeValue(const RespDouble& value) {
        using namespace std::string_view_literals;
        print(","sv, value.value, "\r\n"sv);
    }
    void writeValue(const RespBigInteger& value) {
        using namespace std::string_view_literals;
        print("("sv, value.value, "\r\n"sv);
    }
    void writeValue(const RespMap& value) {
        using namespace std::string_view_literals;
        print("%"sv, value.value.size(), "\r\n"sv);
        for (const auto& [k, v] : value.value) {
            writeValue(k);
            writeValue(v);
        }
    }
    void writeValue(const RespSet& value) {
        using namespace std::string_view_literals;
        print("~"sv, value.value.size(), "\r\n"sv);
        for (const auto& v : value.value) {
            writeValue(v);
        }
    }
    void writeValue(const RespPush& value) {
        using namespace std::string_view_literals;
        print(">"sv, value.value.size(), "\r\n"sv);
        for (const auto& v : value.value) {
            writeValue(v);
        }
    }

    void writeValue(const RespValue& value){
      std::visit([&](const auto& v) { writeValue(v); }, value);
    }
  };

  void serve(int client_fd){
      BufferedReader reader{
      .reader = [client_fd](char *buffer, size_t size) { return read(client_fd, buffer, size); },
  };

    BufferedWriter _writer{
    .writer = [client_fd](const char* buffer, size_t size) { return write(client_fd, buffer, size); },
  };
    std::recursive_mutex mutex;
    auto reply = [&](const RespValue& value) {
      std::lock_guard lock(mutex); 
      _writer.writeValue(value);
      _writer.flush();
    };
     while (true) {
     try {
         // reads a command from client
         RespValue value = reader.readValue();
         RespArray& command = std::get<RespArray>(value);
         std::vector<std::string> commandLine;
         for (const auto& v : command.value) {
             commandLine.emplace_back(std::get<RespString>(v).value);
         }
         // prints the command
         std::cout << "Command: ";
         for (const auto& c : commandLine) {
             std::cout << c << ' ';
         }
         std::cout << '\n';
         if (commandLine.empty()) {
             reply(RespError{
                 .type = RespError::Type::SimpleError,
                 .value = "ERR Empty command",
                 });
             continue;
         }
         // handles the command
         using namespace std::string_view_literals;
         std::string commandName = commandLine[0];
         std::transform(commandName.begin(), commandName.end(), commandName.begin(), ::toupper);
         if (commandName == "COMMAND"sv) {
             reply(RespArray{
                 .value =
                     {
                         RespString{
                             .type = RespString::Type::SimpleString,
                             .value = "COMMAND",
                         },
                         RespString{
                             .type = RespString::Type::SimpleString,
                             .value = "PING",
                         },
                         RespString{
                             .type = RespString::Type::SimpleString,
                             .value = "ECHO",
                         },
                     },
                 });
         }
         else if (commandName == "PING"sv) {
             if (commandLine.size() == 1) {
                 reply(RespString{
                     .type = RespString::Type::SimpleString,
                     .value = "PONG",
                     });
             }
             else {
                 reply(RespString{
                     .type = RespString::Type::BulkOrVerbatimString,
                     .value = commandLine[1],
                     });
             }
         }
         else if (commandName == "ECHO"sv) {
             if (commandLine.size() == 1) {
                 reply(RespError{
                     .type = RespError::Type::SimpleError,
                     .value = "ERR ECHO requires an argument",
                     });
             }
             else {
                 reply(RespString{
                     .type = RespString::Type::BulkOrVerbatimString,
                     .value = commandLine[1],
                     });
             }
         }
         else {
             reply(RespError{
                 .type = RespError::Type::SimpleError,
                 .value = "ERR Unknown command",
                 });
         }
     }
    catch (const IOError& e) {
      break;
    }
  }
}}

int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";
  while (true) {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    if (client_fd < 0) {
      std::cerr << "Failed to accept client connection\n";
    } else {
      std::thread(Redis::serve, client_fd).detach();
    }
  }
  close(server_fd);
  return 0;
}