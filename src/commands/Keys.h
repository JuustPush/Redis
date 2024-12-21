#include "Command.h"
#include "Storage.h"
#include <memory>
#include <optional>
#include <string>


namespace commands {
struct Keys : public Command {
  Keys(const std::shared_ptr<const KVStorage> data)
         : Command(false), data_(data) {}

  ~Keys() = default;

  std::optional<std::string>
  inner_handle(const std::span<const std::string> &params,
               Session* session) override;

 private:
  const std::shared_ptr<const KVStorage> data_;
};
} // namespace commands