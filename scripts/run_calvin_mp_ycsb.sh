results_dir=~/star/exp_results/gc_calvin_mp_ycsb
mkdir -p $results_dir
cd $results_dir
script_name="gc_calvin_mp_ycsb_run"
bsize=400
logging_latency=0
nohup gcloud compute ssh --zone us-central1-a node2  --command "cd star;logging_latency=$logging_latency bsize=$bsize ./$script_name.sh" > $script_name-2.log &
nohup gcloud compute ssh --zone us-central1-a node3  --command "cd star;logging_latency=$logging_latency bsize=$bsize ./$script_name.sh" > $script_name-3.log &

log_path=$results_dir/$script_name-1.log
cd ~/star
logging_latency=$logging_latency bsize=$bsize ./$script_name.sh > $log_path 2>&1
