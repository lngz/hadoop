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

FILE_PREFIX="/nanke/$DATE/100_01_"

ALLFILE="$FILE_PREFIX$DATE$TIME*"

HADOOP=/hadoop-1.0.3/bin/hadoop
PIG='/pig-0.10.0/bin/pig'
#---------------------------------------------------------
# clean result directory
$HADOOP fs -rmr /result/$DATE$TIME

#--------------------------------------------------------
# generate pig latin program 
PIG_CMD="dns_$DATE$TIME.pig"
DOMAIN_LIST="domain_list"
cat <<EOF > $PIG_CMD
-- load all file to processing
a = load '$ALLFILE' using PigStorage('|') as (sourceip:chararray, domainname:chararray, requestdate:chararray, destinationip:chararray, resultcode:chararray);
-- generate report 1-8
rpt1_1 = group a by (domainname, resultcode);
rpt1 = foreach rpt1_1 generate flatten(group), COUNT(a);
rpt3_1 = group a by (sourceip, resultcode);
rpt3 = foreach rpt3_1 generate flatten(group), COUNT(a);
rpt6_1 = group a by (requestdate, resultcode);
rpt6 = foreach rpt6_1 generate flatten(group), COUNT(a);
rpt4_1 = group a by (domainname, sourceip);
rpt4 = foreach rpt4_1 generate flatten(group), COUNT(a);
rpt7_1 = group a by (domainname, destinationip);
rpt7 = foreach rpt7_1 generate flatten(group), COUNT(a) as cnt;
--
--rpt8_1 = group a by (domainname, requestdate);
--rpt8 = foreach rpt8_1 generate flatten(group), COUNT(a) as cnt;
--
EOF

if [ -e $DOMAIN_LIST ] 
then
    LATIN_PRG="b = FILTER a by "
    FIRSTLINE=0
    while read DOMAIN
    do
	if [ $FIRSTLINE == 0 ]  
	then
	    LATIN_PRG="$LATIN_PRG (domainname == '$DOMAIN')"
	    FIRSTLINE=1
	else
	    LATIN_PRG="$LATIN_PRG OR (domainname == '$DOMAIN')"
	fi
	
    done < $DOMAIN_LIST
    LATIN_PRG="$LATIN_PRG;" 
    echo $LATIN_PRG >>$PIG_CMD
    echo 'rpt8_1 = group b by (domainname, requestdate);' >>$PIG_CMD
    echo 'rpt8 = foreach rpt8_1 generate flatten(group), COUNT(b) as cnt;' >>$PIG_CMD
else
    echo 'rpt8_1 = group a by (domainname, requestdate);' >>$PIG_CMD
    echo 'rpt8 = foreach rpt8_1 generate flatten(group), COUNT(a) as cnt;' >>$PIG_CMD
fi

cat <<EOF >>$PIG_CMD

-- store all result to directory with timestemp
store rpt1 into '/result/$DATE$TIME/dnstest_rpt1' using PigStorage('\t');
store rpt3 into '/result/$DATE$TIME/dnstest_rpt3' using PigStorage('\t');
store rpt6 into '/result/$DATE$TIME/dnstest_rpt6' using PigStorage('\t');
store rpt4 into '/result/$DATE$TIME/dnstest_rpt4' using PigStorage('\t');
store rpt7 into '/result/$DATE$TIME/dnstest_rpt7' using PigStorage('\t');
store rpt8 into '/result/$DATE$TIME/dnstest_rpt8' using PigStorage('\t');
EOF
#if [ -e $DOMAIN_LIST ]
#then
#    echo 'store rpt8 into '/result/$DATE$TIME/dnstest_rpt8' using PigStorage('\t');' >>$PIG_CMD
#fi

#----------------------------------------------------------------------------------
# execute pig processing
$PIG $PIG_CMD

if [ $? == 0 ]
then
    rm -rf $PIG_CMD
fi

#----------------------------------------------------------------------------------
# dump the result to hbase

hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt1 /hfile/rpt1
hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt3 /hfile/rpt3
hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt6 /hfile/rpt6
hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt4 /hfile/rpt4
hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt7 /hfile/rpt7
hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt8 /hfile/rpt8

echo "disable 'rpt1_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "disable 'rpt3_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "disable 'rpt6_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "disable 'rpt4_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "disable 'rpt7_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "disable 'rpt8_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell

echo "drop 'rpt1_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "drop 'rpt3_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "drop 'rpt6_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "drop 'rpt4_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "drop 'rpt7_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell
echo "drop 'rpt8_$DATE$TIME'"|/hbase-0.94.0/bin/hbase shell


echo "create 'rpt1_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell
echo "create 'rpt3_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell
echo "create 'rpt6_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell
echo "create 'rpt4_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell
echo "create 'rpt7_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell
echo "create 'rpt8_$DATE$TIME','f1'"|/hbase-0.94.0/bin/hbase shell


hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt1 rpt1_$DATE$TIME
hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt3 rpt3_$DATE$TIME
hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt6 rpt6_$DATE$TIME
hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt4 rpt4_$DATE$TIME
hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt7 rpt7_$DATE$TIME
hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt8 rpt8_$DATE$TIME

#if [ -e $DOMAIN_LIST ]
#then
#    hadoop jar ~/workspace/hfile_dns/bulk.jar bulk_01 /result/$DATE$TIME/dnstest_rpt8 /hfile/rpt8
#    hadoop jar /hbase-0.94.0/hbase-0.94.0.jar completebulkload /hfile/rpt8 rpt8_$DATE$TIME
#fi

#*******************************************************************************
# clean midle file
# clean hfile.
hadoop fs -rmr /hfile


