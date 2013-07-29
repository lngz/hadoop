a = load '/dns_cache' using PigStorage('|') as (sourceip:chararray, domainname:chararray, requestdate:chararray, destinationip:chararray, resultcode:chararray);

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

rpt8_1 = group a by (domainname, requestdate);
rpt8 = foreach rpt8_1 generate flatten(group), COUNT(a) as cnt;

store rpt1 into '/result/dnstest_rpt1' using PigStorage(',');
store rpt3 into '/result/dnstest_rpt3' using PigStorage(',');
store rpt6 into '/result/dnstest_rpt6' using PigStorage(',');
store rpt4 into '/result/dnstest_rpt4' using PigStorage(',');
store rpt7 into '/result/dnstest_rpt7' using PigStorage(',');
store rpt8 into '/result/dnstest_rpt8' using PigStorage(',');

