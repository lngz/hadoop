
register myudf.jar;
a= load './test' AS (name: chararray, test: chararray);
B = FOREACH a GENERATE myudfs.UPPER(name);
--c = FOREACH a GENERATE group, COUNT(a);
dump B;
