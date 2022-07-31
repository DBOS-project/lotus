results_dir=~/star/exp_results/gc_lotus_mp_ycsb_sync
mkdir -p  $results_dir
cd $results_dir
script_name="gc_lotus_mp_ycsb_run"
bsize=6 # one tranasction per worker
lotus_async_repl=false # active-active replication
partition_num=18
granule_count=2000
cross_part_num=5
logging_latency=0
nohup gcloud compute ssh --zone us-central1-a node2  --command "cd star;logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh" > $script_name-2.log &
nohup gcloud compute ssh --zone us-central1-a node3  --command "cd star;logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh" > $script_name-3.log &
nohup gcloud compute ssh --zone us-central1-a node4  --command "cd star;logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh" > $script_name-4.log &
nohup gcloud compute ssh --zone us-central1-a node5  --command "cd star;logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh" > $script_name-5.log &
nohup gcloud compute ssh --zone us-central1-a node6  --command "cd star;logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh" > $script_name-6.log &


log_path=$results_dir/$script_name-1.log
cd ~/star
logging_latency=$logging_latency cross_part_num=$cross_part_num  lotus_async_repl=$lotus_async_repl granule_count=$granule_count partition_num=$partition_num bsize=$bsize  ./$script_name.sh > $log_path 2>&1
