#!/bin/bash
#*******************************************************************************
#* $Header$
#*******************************************************************************
#
# system     :
# programid  :
# description:
#
# argument :
# xxxx.sh ARG1 ARG2 
#
#  ARG1: date
#
#  ARG2: hour
#
#
# history:
# time     type company  auth       description
# -------- ---- -------- ---------- -----------------------------------------------
# 20120806 init  systex   shiziliang initial
#
#-------------------------------------------------------------------------------
# CopyRight by systex group(r) 
#
#*******************************************************************************
DATE=$1
TIME=$2

if [ "$#" -lt 1 ]
then
    echo "USAGE:"
    echo "$0 ARG1 [ARG2] "
    echo '    ARG1: date '
    echo '    ARG2: hour '   
    exit 1
fi

HADOOP=/hadoop-1.0.3/bin/hadoop

MONITOR_DIR=/home/splunk432/result/$DATE$TIME

mkdir -p $MONITOR_DIR

$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt1 $MONITOR_DIR
$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt3 $MONITOR_DIR
$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt4 $MONITOR_DIR
$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt6 $MONITOR_DIR
$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt7 $MONITOR_DIR
$HADOOP fs -get /result/$DATE$TIME/dnstest_rpt8 $MONITOR_DIR

#splunk add monitor /home/splunk432/result/dnstest_rpt1 -sourcetype summary_domain_resultcode -auth admin:changeme
exit 0
~/splunk/bin/splunk add index $DATE
~/splunk/bin/splunk add monitor $MONITOR_DIR/dnstest_rpt1 -sourcetype summary_sourceip_resultcode -auth admin:changeme -index $DATE
~/splunk/bin/splunk add monitor $MONITOR_DIR/dnstest_rpt1 -sourcetype summary_sourceip_resultcode -auth admin:changeme -index $DATE
~/splunk/bin/splunk add monitor $MONITOR_DIR/dnstest_rpt1 -sourcetype summary_sourceip_resultcode -auth admin:changeme -index $DATE
~/splunk/bin/splunk add monitor $MONITOR_DIR/dnstest_rpt1 -sourcetype summary_sourceip_resultcode -auth admin:changeme -index $DATE

exit 0
