import sys

ips = [line.strip() for line in open("ips.txt", "r")]

id = int(sys.argv[1])
port = int(sys.argv[2]) 

#protocols = ["SiloGC", "TwoPLGC"]
#protocols = ["Calvin"]
protocols = ["Star"]
ns = [8, 7, 6, 5, 4, 3, 2]

for n in ns:
  if id >= n:
    break
  ins = [line.split("\t")[0] for line in ips[0:n]]
  outs = [line.split("\t")[1] for line in ips[0:n]]
  for protocol in protocols:
    for i in range(3):
      cmd = ""
      for j in range(n):
        if j > 0:
          cmd += ";"
        if id == j:
          cmd += ins[j] + ":" + str(port+i)
        else:
          cmd += outs[j] + ":" + str(port+i)
      print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --partitioner=hash2 --threads=12 --read_write_ratio=90 --cross_ratio=10 --batch_size=1000 --batch_flush=200' % (id, cmd, protocol, 12*n))
      #print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --batch_size=10000 --replica_group=%d --lock_manager=2 --read_write_ratio=90 --cross_ratio=10' % (id, cmd, protocol, 12*n, n))

    for i in range(3):
      cmd = ""
      for j in range(n):
        if j > 0:
          cmd += ";"
        if id == j:
          cmd += ins[j] + ":" + str(port+i)
        else:
          cmd += outs[j] + ":" + str(port+i)
  
      print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --partitioner=hash2 --threads=12 --query=mixed --neworder_dist=10 --payment_dist=15 --batch_size=1000' % (id, cmd, protocol, 12*n))
      #print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --batch_size=10000 --replica_group=%d --lock_manager=4 --query=mixed --neworder_dist=10 --payment_dist=15' % (id, cmd, protocol, 12*n, n))
