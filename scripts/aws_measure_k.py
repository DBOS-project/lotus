import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["Star", "SiloGC", "TwoPLGC"]

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

ix = 0


for protocol in protocols: 
  cmd = get_cmd(n, ix)
  ix += 1
  print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=10 --batch_flush=200' % (id, cmd, protocol, 12*n))
      

for protocol in protocols: 
  cmd = get_cmd(n, ix)
  ix += 1
  print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=mixed --neworder_dist=10 --payment_dist=15' % (id, cmd, protocol, 12*n))   