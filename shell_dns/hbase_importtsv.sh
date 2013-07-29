echo "create 'domain','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:domain,info:resultcode,info:count '-Dimporttsv.separator=,' domain /result/dnstest_rpt1/part-r-00000

echo "create 'sourceip','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:sourceip,info:resultcode,info:count '-Dimporttsv.separator=,' sourceip /result/dnstest_rpt3/part-r-00000

echo "create 'requestdate','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:requestdate,info:resultcode,info:count '-Dimporttsv.separator=,' requestdate /result/dnstest_rpt6/part-r-00000

echo "create 'domain_sourceip','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:domain,info:sourceip,info:count '-Dimporttsv.separator=,' domain_sourceip /result/dnstest_rpt4/part-r-00000

echo "create 'domain_destinationip','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:domain,info:destinationip,info:count '-Dimporttsv.separator=,' domain_destinationip /result/dnstest_rpt7/part-r-00000

echo "create 'domain_requestdate','info'" |/hbase-0.94.0/bin/hbase shell
/hadoop-1.0.3/bin/hadoop jar /hbase-0.94.0/hbase-0.94.0.jar  importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:domain,info:requestdate,info:count '-Dimporttsv.separator=,' domain_requestdate /result/dnstest_rpt8/part-r-00000

