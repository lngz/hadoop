a = load '/dns_cache' using PigStorage('|') as (sourceip:chararray, domainname:chararray, requestdate:chararray, destinationip:chararray, resultcode:chararray);

rpt1_1 = group a by (domainname, resultcode);
rpt1 = foreach rpt1_1 generate flatten(group), COUNT(a);
rpt1_hbase = foreach rpt1 generate CONCAT($0,$1) as keys, $0 as domain, $1 as resultcode, $2 as count;
rpt3_1 = group a by (sourceip, resultcode);
rpt3 = foreach rpt3_1 generate flatten(group), COUNT(a);
rpt3_hbase = foreach rpt3 generate CONCAT($0,$1) as keys, $0 as sourceip, $1 as resultcode, $2 as count;
rpt6_1 = group a by (requestdate, resultcode);
rpt6 = foreach rpt6_1 generate flatten(group), COUNT(a);
rpt6_hbase = foreach rpt6 generate CONCAT($0,$1) as keys, $0 as requestdate, $1 as resultcode, $2 as count;
rpt4_1 = group a by (domainname, sourceip);
rpt4 = foreach rpt4_1 generate flatten(group), COUNT(a);
rpt4_hbase = foreach rpt4 generate CONCAT($0,$1) as keys, $0 as domainname, $1 as sourceip, $2 as count;

rpt7_1 = group a by (domainname, destinationip);
rpt7 = foreach rpt7_1 generate flatten(group), COUNT(a) as cnt;
rpt7_hbase = foreach rpt7 generate CONCAT($0,$1) as keys, $0 as domainname, $1 as destinationip, $2 as count;

rpt8_1 = group a by (domainname, requestdate);
rpt8 = foreach rpt8_1 generate flatten(group), COUNT(a) as cnt;
rpt8_hbase = foreach rpt8 generate CONCAT($0,$1) as keys, $0 as domainname, $1 as requestdate, $2 as count;

store rpt1_hbase into '/result/dnstest_rpt1' using PigStorage(',');
store rpt3_hbase into '/result/dnstest_rpt3' using PigStorage(',');
store rpt6_hbase into '/result/dnstest_rpt6' using PigStorage(',');
store rpt4_hbase into '/result/dnstest_rpt4' using PigStorage(',');
store rpt7_hbase into '/result/dnstest_rpt7' using PigStorage(',');
store rpt8_hbase into '/result/dnstest_rpt8' using PigStorage(',');

