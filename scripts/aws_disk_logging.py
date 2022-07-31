import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

def get_cmd(n, i):
  cmd = ""
  for j in range(n):
    if j > 0:
      cmd += ";"
    if id == j:
      cmd += ins[j] + ":" + str(port+i)
    else:
      cmd += outs[j] + ":" + str(port+i)
  return cmd
      

#for i in range(3):
#  cmd = get_cmd(n, i)
#  print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --read_write_ratio=90 --cross_ratio=10 --batch_flush=200' % (id, cmd, 12*n))  
  

#for i in range(3):
#  cmd = get_cmd(n, i)
#  print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --read_write_ratio=90 --cross_ratio=10 --batch_flush=200 --log_path=/home/ubuntu/star.logging' % (id, cmd, 12*n))  

      
#for i in range(3):
#  cmd = get_cmd(n, i)
#  print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --query=mixed --neworder_dist=10 --payment_dist=15' % (id, cmd, 12*n))  
  
for i in range(3):
  cmd = get_cmd(n, i)
  print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --query=mixed --neworder_dist=10 --payment_dist=15 --log_path=/home/ubuntu/star.logging' % (id, cmd, 12*n))   
      

