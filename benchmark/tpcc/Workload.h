//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include <string>
#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace star {

namespace tpcc {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id), db(db), random(random),
        partitioner(partitioner) {}

  static uint64_t next_transaction_id(uint64_t coordinator_id) {
    constexpr int coordinator_id_offset = 32;
    static std::atomic<int64_t> tid_static{1};
    auto tid = tid_static.fetch_add(1);
    return (coordinator_id << coordinator_id_offset) | tid;
  }

  std::unique_ptr<TransactionType> next_transaction(ContextType &context,
                                                    std::size_t partition_id,
                                                    std::size_t worker_id,
                                                    std::size_t granule_id = 0) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    static std::atomic<uint64_t> tid_cnt(0);
    long long transactionId = tid_cnt.fetch_add(1);
    auto random_seed = Time::now();


    std::string transactionType;
    random.set_seed(random_seed);
    if (context.workloadType == TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner);
        transactionType = "TPCC NewOrder";
      } else {
        p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner);
        transactionType = "TPCC Payment";
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
                                                  db, context, random,
                                                  partitioner);
      transactionType = "TPCC NewOrder";
    } else {
      p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner);
      transactionType = "TPCC NewOrder";
    }
    p->txn_random_seed_start = random_seed;
    p->transaction_id = next_transaction_id(coordinator_id);
    return p;
  }

  std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string & data) {
    Decoder decoder(data);
    uint64_t seed;
    uint32_t txn_type;
    std::size_t ith_replica;
    std::size_t partition_id;
    int64_t transaction_id;
    uint64_t straggler_wait_time;
    decoder >> transaction_id >> txn_type >> straggler_wait_time >> ith_replica >> seed >> partition_id;
    RandomType random;
    random.set_seed(seed);

    if (txn_type == 0) {
      auto p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
             ith_replica);
      p->txn_random_seed_start = seed;
      p->transaction_id = transaction_id;
      p->straggler_wait_time = straggler_wait_time;
      p->deserialize_lock_status(decoder);
      return p;
    } else {
      auto p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, ith_replica);
      p->txn_random_seed_start = seed;
      p->transaction_id = transaction_id;
      p->straggler_wait_time = straggler_wait_time;
      p->deserialize_lock_status(decoder);
      return p;
    }
  }

private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace tpcc
} // namespace star
