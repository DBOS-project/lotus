import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["Silo", "TwoPL"]
ratios = [10, 50, 90]

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


for protocol in protocols: 
  for i in range(len(ratios)):
    ratio = ratios[i]
    cmd = get_cmd(n, i)
    print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=%d --batch_flush=20' % (id, cmd, protocol, 12*n, ratio))
      
for protocol in protocols: 
  for i in range(len(ratios)):
    ratio = ratios[i]
    cmd = get_cmd(n, i)
    print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=mixed --neworder_dist=%d --payment_dist=%d --batch_flush=20' % (id, cmd, protocol, 12*n, ratio, ratio))   


cmd = get_cmd(n, 0)      
print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=%d --batch_flush=20' % (id, cmd, "Star", 12*n, ratio))
cmd = get_cmd(n, 1)  
print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=%d --batch_flush=20' % (id, cmd, "SiloGC", 12*n, ratio))
cmd = get_cmd(n, 2)  
print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=%d --batch_flush=20' % (id, cmd, "TwoPLGC", 12*n, ratio))