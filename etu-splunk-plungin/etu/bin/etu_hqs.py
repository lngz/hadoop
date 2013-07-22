import subprocess
import json
import os, sys, time, datetime
import splunk.Intersplunk as isp
import splunk.util as util

########################

key = '_raw'
tableName = ''

if len(sys.argv) >= 3:
    tableName = sys.argv[1]
    key = sys.argv[2]

########################


#results,dummyresults,settings = isp.getOrganizedResults()

results = isp.readResults(None, None, True)

rowkey = []

for field in results:
    if field.get(key, None):
        rowkey.append(field.get(key))

#############################
#
#f = open('/root/input','w')
#
#f.write(str(rowkey))
#
#f.close()
#
#############################

args = 'select_list ' + tableName + ' '

for arg in rowkey:
    args = args + arg + ' '

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
