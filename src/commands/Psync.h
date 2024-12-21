#include "Command.h"
#include "ReplicationInfo.h"
#include <memory>
#include <optional>
#include <string>

namespace commands {
struct Psync : public Command {
  Psync(std::shared_ptr<ReplicationInfo> rep_info)
      : Command(false), rep_info_(rep_info) {}

  ~Psync() = default;

  std::optional<std::string> inner_handle(const std::span<const std::string> &params,
                                          Session *session) override;

  void after_write(const std::span<const std::string> &params,
                   Session *session) override;

private:
  std::shared_ptr<ReplicationInfo> rep_info_;
};
} // namespace commands