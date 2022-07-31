import sys
import os

ips = [line.strip() for line in open("instance_names_half.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

port = int(sys.argv[1]) 
script = sys.argv[2]
gc_zone_name = sys.argv[3]
script_base = os.path.basename(script)
script_no_extension = os.path.splitext(script_base)[0]
print(script_no_extension)

for i in range(n):
  os.system("python %s %d %d > run.sh" % (script, i, port))
  os.system("chmod u+x run.sh")
  os.system("gcloud compute scp --zone %s run.sh %s:~/star/%s_run.sh" % (gc_zone_name, outs[i], script_no_extension))
  #os.system("scp run.sh ubuntu@%s:~/star/run.sh" % outs[i])
