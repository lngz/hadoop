import subprocess
import json
import os, sys, time, datetime
import splunk.Intersplunk as isp
import splunk.util as util

############################

args_list = sys.argv[1:]

args = ''

for arg in args_list:
    args = args + arg + ' '
    
    
############################
#
#f = open('/root/cmd','w')
#
#f.write(args)
#
#f.close()
#
############################

process = subprocess.Popen('./etu_hq.sh ' + args, shell=True, stdout=subprocess.PIPE)

#f = open('/root/result','w')

while(True):
    
    line = process.stdout.readline()
    
    if line:
        
        tmp = []
        tmp.append(json.loads(line))
        
#        f.write(str(tmp))
        
        isp.outputResults(json.loads(line))
    else:
        break

#f.close()
    