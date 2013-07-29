a = load '/dns_cache/100_01_20120618140001' using PigStorage('|') as (sourceip:chararray, domainname:chararray, requestdate:chararray, destinationip:chararray, resultcode:chararray);

rpt1_1 = group a by (domainname, resultcode);
rpt1 = foreach rpt1_1 generate flatten(group), COUNT(a);
rpt3_1 = group a by (sourceip, resultcode);
rpt3 = foreach rpt3_1 generate flatten(group), COUNT(a);
rpt6_1 = group a by (requestdate, resultcode);
rpt6 = foreach rpt6_1 generate flatten(group), COUNT(a);
rpt4_1 = group a by (domainname, sourceip);
rpt4 = foreach rpt4_1 generate flatten(group), COUNT(a);

store rpt1 into '/result/dnstest_rpt1' using PigStorage(',');
store rpt3 into '/result/dnstest_rpt3' using PigStorage(',');
store rpt6 into '/result/dnstest_rpt6' using PigStorage(',');
store rpt4 into '/result/dnstest_rpt4' using PigStorage(',');

