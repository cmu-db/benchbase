SERVER=127.0.0.1
PORT=3306
java -cp ./build/classes:./lib/commons-lang-2.5.jar:./lib/mysql-connector-java-5.1.10-bin.jar -Dnwarehouses=32 -Dnterminals=160 -Ddriver=com.mysql.jdbc.Driver -Dconn=jdbc:mysql://$SERVER:$PORT/tpcc -Duser=root client.TPCCRateLimitedFromFile  config/config.txt $1 > transactions.csv
