//
// Created by Xinjing on 9/12/21.
//

#pragma once

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include "common/WALLogger.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star {
class HStoreTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  HStoreTransaction(std::size_t coordinator_id, std::size_t partition_id,
                   Partitioner &partitioner, std::size_t ith_replica)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner), ith_replica(ith_replica) {
    reset();
    tries = 0;
  }
  
  virtual ~HStoreTransaction() = default;

  void set_logger(WALLogger * logger) {
    this->logger = logger;
  }

  WALLogger * get_logger() {
    return this->logger;
  }

  std::size_t commit_unlock_time_us = 0;
  std::size_t commit_work_time_us = 0;
  std::size_t commit_write_back_time_us = 0;
  std::size_t remote_work_time_us = 0;
  std::size_t local_work_time_us = 0;
  std::size_t stall_time_us = 0; // Waiting for locks (partition-level or row-level) due to conflicts
 
  std::size_t commit_prepare_time_us = 0;
  std::size_t commit_persistence_time_us = 0;
  std::size_t commit_replication_time_us = 0;
  virtual void record_commit_replication_time(uint64_t us) {
    commit_replication_time_us += us;
  }

  virtual size_t get_commit_replication_time() {
    return commit_replication_time_us;
  }
  virtual void record_commit_persistence_time(uint64_t us) {
    commit_persistence_time_us += us;
  }

  virtual size_t get_commit_persistence_time() {
    return commit_persistence_time_us;
  }
  
  virtual void record_commit_prepare_time(uint64_t us) {
    commit_prepare_time_us += us;
  }

  virtual size_t get_commit_prepare_time() {
    return commit_prepare_time_us;
  }

  virtual void record_remote_work_time(uint64_t us) {
    remote_work_time_us += us;
  }

  virtual size_t get_remote_work_time() {
    return remote_work_time_us;
  }
  
  virtual void record_local_work_time(uint64_t us) {
    local_work_time_us += us;
  }

  virtual size_t get_local_work_time() {
    return local_work_time_us;
  }

  virtual void record_commit_work_time(uint64_t us) {
    commit_work_time_us += us;
  }

  virtual size_t get_commit_work_time() {
    return commit_work_time_us;
  }

  virtual void record_commit_write_back_time(uint64_t us) {
    commit_write_back_time_us += us;
  }

  virtual size_t get_commit_write_back_time() {
    return commit_write_back_time_us;
  }

  virtual void record_commit_unlock_time(uint64_t us) {
    commit_unlock_time_us += us;
  }

  virtual size_t get_commit_unlock_time() {
    return commit_unlock_time_us;
  }

  virtual void set_stall_time(uint64_t us) {
    stall_time_us = us;
  }

  virtual size_t get_stall_time() {
    return stall_time_us;
  }

  void reset() {
    abort_lock_local_read = false;
    pendingResponses = 0;
    network_size = 0;
    abort_lock = false;
    abort_read_validation = false;
    local_validated = false;
    si_in_serializable = false;
    distributed_transaction = false;
    execution_phase = false;
    synchronous = true;
    finished_commit_phase = false;
    abort_lock_lock_released = false;
    release_lock_called = false;
    operation.clear();
    readSet.clear();
    ++tries;
    writeSet.clear();
    lock_request_responded = false;
    command_written = false;
  }

  virtual int32_t get_partition_count() = 0;

  virtual int32_t get_partition(int i) = 0;

  virtual int32_t get_partition_granule_count(int i) = 0;

  virtual int32_t get_granule(int partition_id, int j) = 0;

  virtual bool is_single_partition() = 0;

  // Which replica this txn runs on
  virtual const std::string serialize(std::size_t ith_replica = 0) = 0;

  virtual ITable* getTable(size_t tableId, size_t partitionId) {
    return get_table(tableId, partitionId);
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value, bool readonly,
                          int granule_id = 0) {
    if (execution_phase) {
      return;
    }
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_granule_id(granule_id);

    readKey.set_key(&key);
    readKey.set_value(&value);
    if (readonly) {
      readKey.set_local_index_read_bit();
    }
    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value,
                       int granule_id = 0) {
    if (execution_phase) {
      return;
    }
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_granule_id(granule_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value,
                         int granule_id = 0) {
    if (execution_phase) {
      return;
    }
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);
    readKey.set_granule_id(granule_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_write_lock_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value,
                         int granule_id = 0) {
    if (execution_phase) {
      return;
    }
    TwoPLRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);
    writeKey.set_granule_id(granule_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  bool process_requests(std::size_t worker_id, bool last_call_in_transaction = true) {
    // cannot use unsigned type in reverse iteration
    ScopedTimer t_local_work([&, this](uint64_t us) {
      this->record_local_work_time(us);
    });

    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_lock_request_bit() &&
          !readSet[i].get_write_lock_request_bit()) {
        break;
      }

      const TwoPLRWKey &readKey = readSet[i];
      bool success, remote;
      lock_request_handler(
          readKey.get_table_id(), readKey.get_partition_id(), readKey.get_granule_id(), i,
          readKey.get_key(), readKey.get_value(),
          readSet[i].get_local_index_read_bit(),
          readSet[i].get_write_lock_request_bit(), success, remote);
      
      if (!remote) {
        if (!success) {
          abort_lock = true;
        }
      }
      readSet[i].clear_read_lock_request_bit();
      readSet[i].clear_write_lock_request_bit();
    }
    t_local_work.end();
    if (synchronous) {
      if (pendingResponses > 0) {
        ScopedTimer t_remote_work([&, this](uint64_t us) {
          this->record_remote_work_time(us);
        });
        message_flusher();
        while (pendingResponses > 0) {
          remote_request_handler(0);
        }
      }
      if (execution_phase == false && last_call_in_transaction)
        execution_phase = true;
    } else {
      message_flusher();
    }
    return false;
  }

  TwoPLRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const TwoPLRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const TwoPLRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;
  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
  bool distributed_transaction;
  bool execution_phase = false;

  // table id, partition id, key, value, local_index_read?, write_lock?,
  // success?, remote?
  std::function<void(std::size_t, std::size_t, std::size_t, uint32_t, const void *,
                         void *, bool, bool, bool &, bool &)>
      lock_request_handler;
  // processed a request?
  std::function<std::size_t(std::size_t)> remote_request_handler;

  std::function<void()> message_flusher;

  std::function<ITable*(std::size_t, std::size_t)> get_table;

  Partitioner &partitioner;
  std::size_t ith_replica;
  Operation operation;
  std::vector<TwoPLRWKey> readSet, writeSet;
  WALLogger * logger = nullptr;
  int initiating_cluster_worker_id = -1;
  uint64_t txn_random_seed_start = 0;
  uint64_t txn_cmd_log_lsn = 0;
  int64_t initiating_transaction_id = 0;
  int64_t transaction_id = 0;
  bool reordered_in_the_queue = false;
  bool replicated_sp = false;
  bool being_replayed = false;
  int64_t tries = 0;
  bool synchronous = true;
  bool abort_no_retry = false;
  bool finished_commit_phase = false;
  bool abort_lock_lock_released = false;
  bool release_lock_called = false;
  int replay_queue_lock_id = -1;
  bool abort_lock_local_read = false;
  int64_t execution_done_latency = 0;
  //int64_t commit_initiated_latency = 0;
  std::chrono::steady_clock::time_point lock_issue_time;
  bool lock_request_responded = false;
  //int64_t first_lock_response_latency = 0;
  //int64_t first_lock_request_arrive_latency = 0;
  //int64_t first_lock_request_processed_latency = 0;
  int abort_lock_owned_by_others = 0;
  int abort_lock_owned_by_no_one = 0;
  int abort_lock_queue_len_sum = 0;
  //int no_in_group = 0;
  uint64_t straggler_wait_time = 0;
  bool command_written = false;
  int granules_left_to_lock = 0;
  int64_t position_in_log;
};
} // namespace star