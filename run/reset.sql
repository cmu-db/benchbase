#-------------RESET--------------------------

TRUNCATE TABLE page;
INSERT INTO page SELECT * FROM page_backup;
DELETE FROM revision WHERE rev_id > (SELECT maxid FROM value_backup WHERE table_name = "revision");
DELETE FROM text WHERE old_id > (SELECT maxid FROM value_backup WHERE table_name = "text"); -- this can be very slow
DELETE FROM recentchanges WHERE rc_id > (SELECT 10*count(*) from page);
DELETE FROM logging WHERE log_id > (SELECT 10*count(*) from page);