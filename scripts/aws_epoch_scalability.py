import sys

ips = [line.strip() for line in open("ips.txt", "r")]

id = int(sys.argv[1])
port = int(sys.argv[2]) 

#ns = [16, 14, 12, 10, 8, 6, 4, 2]
#ns = [16, 14, 12, 10]
ns = [8, 6, 4, 2]
group_times = [10, 20, 50, 100, 200]


for n in ns:
  if id >= n:
    break
  ins = [line.split("\t")[0] for line in ips[0:n]]
  outs = [line.split("\t")[1] for line in ips[0:n]]
  for t in group_times:
    for i in range(3):
      cmd = ""
      for j in range(n):
        if j > 0:
          cmd += ";"
        if id == j:
          cmd += ins[j] + ":" + str(port+i)
        else:
          cmd += outs[j] + ":" + str(port+i)

      print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --cross_ratio=10 --read_write_ratio=90 --group_time=%d --batch_size=1000 --batch_flush=200' % (id, cmd, 12*n, t))
  
    for i in range(3):
      cmd = ""
      for j in range(n):
        if j > 0:
          cmd += ";"
        if id == j:
          cmd += ins[j] + ":" + str(port+i)
        else:
          cmd += outs[j] + ":" + str(port+i)

      print('./bench_tpcc --logtostderr=1 --id=%d --servers="%s" --protocol=Star --partition_num=%d --threads=12 --query=mixed --neworder_dist=10 --payment_dist=15 --group_time=%d --batch_size=1000' % (id, cmd, 12*n, t))
