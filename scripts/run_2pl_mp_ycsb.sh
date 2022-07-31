results_dir=~star/exp_results/gc_2pl_mp_ycsb
mkdir -p  $results_dir
cd $results_dir
script_name="gc_2pl_mp_ycsb_run"

nohup gcloud compute ssh --zone us-central1-a node2  --command "cd star; ./$script_name.sh" > $script_name-2.log &
nohup gcloud compute ssh --zone us-central1-a node3  --command "cd star; ./$script_name.sh" > $script_name-3.log &
nohup gcloud compute ssh --zone us-central1-a node4  --command "cd star; ./$script_name.sh" > $script_name-4.log &
nohup gcloud compute ssh --zone us-central1-a node5  --command "cd star; ./$script_name.sh" > $script_name-5.log &
nohup gcloud compute ssh --zone us-central1-a node6  --command "cd star;  ./$script_name.sh" > $script_name-6.log &


log_path=$results_dir/$script_name-1.log
cd ~/star
sh ./$script_name.sh > $log_path 2>&1
