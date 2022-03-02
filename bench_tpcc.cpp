#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_bool(operation_replication, false, "use operation replication");
DEFINE_string(query, "neworder", "tpcc query, mixed, neworder, payment");
DEFINE_int32(neworder_dist, 10, "new order distributed.");
DEFINE_int32(payment_dist, 15, "payment distributed.");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release
bool do_tid_check = false;

int main(int argc, char *argv[]) {
  star::tpcc::Random r;
  std::vector<int> cnt(100, 0);
  for (size_t i = 0; i < 1000000; ++i) {
    auto x = r.non_uniform_distribution(8191, 1, 100000) % 1500;
    if (x < cnt.size()) {
      cnt[x]++;
    }
  }
  // for (size_t i = 1; i < cnt.size(); ++i) {
  //   LOG(INFO) << "i " << i << " " << cnt[i];
  // }
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  star::tpcc::Context context;
  SETUP_CONTEXT(context);

  context.operation_replication = FLAGS_operation_replication;

  context.granules_per_partition = FLAGS_granule_count;

  if (FLAGS_query == "mixed") {
    context.workloadType = star::tpcc::TPCCWorkloadType::MIXED;
  } else if (FLAGS_query == "neworder") {
    context.workloadType = star::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;
  } else if (FLAGS_query == "payment") {
    context.workloadType = star::tpcc::TPCCWorkloadType::PAYMENT_ONLY;
  } else {
    CHECK(false);
  }

  context.newOrderCrossPartitionProbability = FLAGS_neworder_dist;
  context.paymentCrossPartitionProbability = FLAGS_payment_dist;

  star::tpcc::Database db;
  db.initialize(context);

  do_tid_check = false;
  star::Coordinator c(FLAGS_id, db, context);
  c.connectToPeers();
  c.start();
  return 0;
}