#include "Command.h"
#include "ReplicationInfo.h"
#include <optional>
#include <string>
#include <memory>

namespace commands
{
  struct Replconf : public Command
  {
    Replconf(std::shared_ptr<ReplicationInfo> replication_info) : Command(false), replication_info_(std::move(replication_info)) {}

    ~Replconf() = default;

    std::optional<std::string>
    inner_handle(const std::span<const std::string> &params,
                 Session *session) override;

  private:
    std::shared_ptr<ReplicationInfo> replication_info_;
  };
} // namespace commands