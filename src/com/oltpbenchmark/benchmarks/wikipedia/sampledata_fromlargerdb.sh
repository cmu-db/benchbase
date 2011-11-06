
#--------------CREATE-------------
sourcedb=wikidb
targetdb=wikidb_100p
samplesize=1000

# create schema from existing schema
echo "CREATE TABLE $targetdb.page LIKE $sourcedb.page;";
echo "CREATE TABLE $targetdb.revision LIKE $sourcedb.revision;";
echo "CREATE TABLE $targetdb.text LIKE $sourcedb.text;";
echo "ALTER TABLE $targetdb.text ADD COLUMN old_page int;" 
echo "CREATE TABLE $targetdb.recentchanges LIKE $sourcedb.recentchanges;";
echo "CREATE TABLE $targetdb.logging LIKE $sourcedb.logging;";
echo "CREATE TABLE $targetdb.user LIKE $sourcedb.user;";
echo "CREATE TABLE $targetdb.user_groups LIKE $sourcedb.user_groups;";
echo "CREATE TABLE $targetdb.watchlist LIKE $sourcedb.watchlist;";
echo "CREATE TABLE $targetdb.page_restrictions LIKE $sourcedb.page_restrictions;";
echo "CREATE TABLE $targetdb.ipblocks LIKE $sourcedb.ipblocks;";

# sample page table
echo "INSERT INTO $targetdb.page (SELECT * FROM $sourcedb.page ORDER BY page_random LIMIT $samplesize);" 

# get corresponding revision and text
echo "INSERT INTO $targetdb.revision (SELECT r.* FROM $targetdb.page p, $source.revision r WHERE r.rev_page = p.page_id);" 
echo "INSERT INTO $targetdb.text (SELECT t.* FROM $targetdb.revision r, $sourcedb.text t WHERE r.rev_text_id=t.old_id);" 

# update text table to contain old_page
echo "UPDATE $targetdb.text t, $targetdb.revision r SET t.old_page = r.rev_page WHERE t.old_id = r.rev_text_id;" 

# extract info about users from page table
echo "INSERT INTO  $targetdb.user SELECT distinct rev_user, rev_user_text, rev_user_text, "","","20101004183130","fake_something@something.com","fake_longoptionslist","20101004183130","b5ded708b23c820ba269da457f3b2cad",null,null,null,null,1 FROM $targetdb.revision WHERE rev_user > 0 GROUP BY rev_user;"

# add whatever we have for groups and page_restrictions
echo "INSERT INTO $targetdb.user_groups SELECT * FROM $sourcedb.user_groups;"
echo "INSERT INTO $targetdb.page_restrictions select r.* from $targetdb.page p, $sourcedb.page_restrictions r where r.pr_page = p.page_id;"

# save the data needed to revert to this state semi-quickly
echo "CREATE TABLE $targetdb.page_backup LIKE $targetdb.page;"
echo "CREATE TABLE $targetdb.value_backup (table_name varchar(255), maxid int(11));"
echo "INSERT INTO $targetdb.page_backup SELECT * FROM $targetdb.page;"
echo "INSERT INTO $targetdb.value_backup SELECT \"text\", max(old_id) FROM $targetdb.text;"
echo "INSERT INTO $targetdb.value_backup SELECT \"revision\", max(rev_id) FROM $targetdb.revision;"
	

