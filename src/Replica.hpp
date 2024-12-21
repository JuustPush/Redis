#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

class Session;

class Replica {
public:
  Replica(std::shared_ptr<Session> session);

  void add_expected_offset(int delta);

  int get_expected_offset() const;

  std::shared_ptr<Session> get_session();

private:
  std::shared_ptr<Session> session_;

  std::atomic<int> expected_offset_{0};
};

class ReplicaManager {
public:
  void add_replica(std::shared_ptr<Session> replica);

  std::vector<std::shared_ptr<Replica>> get_replicas() const;

private:
  std::vector<std::shared_ptr<Replica>> replicas{};
  mutable std::shared_mutex mtx;
};