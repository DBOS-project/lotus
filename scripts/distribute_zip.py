import sys
import os

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

zip = sys.argv[1] 

for ip in outs:
  os.system("scp %s ubuntu@%s:~/" %(zip, ip))
