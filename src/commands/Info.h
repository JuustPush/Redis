#include "Command.h"
#include "ReplicationInfo.h"
#include <memory>

namespace commands {
struct Info : public Command {
  Info(const std::shared_ptr<const ReplicationInfo> replication_info)
      : Command(false), replication_info_(replication_info) {}

  ~Info() = default;

  std::optional<std::string> inner_handle(const std::span<const std::string>& params, Session* session) override;

private:
  const std::shared_ptr<const ReplicationInfo> replication_info_;
};
} // namespace commands