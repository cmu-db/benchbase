user=root
pass=
host=127.0.0.1
port=3306
db=wikidb_100p
tracefile=trace_100p
limit=100000

echo "select concat(concat(page_namespace,\" \"),concat(page_title,\" -\")) FROM page p, revision r WHERE p.page_id = r.rev_page order by rev_id limit $limit"|  mysql -N -u $user -h $host -P $port -p$pass $db > $tracefile.txt 

