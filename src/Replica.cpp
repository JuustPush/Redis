#include "Replica.hpp"

Replica::Replica(std::shared_ptr<Session> session)
    : session_(std::move(session)) {}

void Replica::add_expected_offset(int delta) {
  expected_offset_.fetch_add(delta);
}

int Replica::get_expected_offset() const { return expected_offset_; }

std::shared_ptr<Session> Replica::get_session() { return session_; }

void ReplicaManager::add_replica(std::shared_ptr<Session> replica_session) {
  std::unique_lock<std::shared_mutex> l(mtx);
  replicas.emplace_back(std::make_shared<Replica>(std::move(replica_session)));
}

std::vector<std::shared_ptr<Replica>> ReplicaManager::get_replicas() const {
  std::shared_lock<std::shared_mutex> l(mtx);
  return replicas;
}