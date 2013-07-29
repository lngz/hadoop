rpt = foreach rpt1 generate CONCAT($0,$1) as keys, $0 as domain, $1 as status, $2 as count;

