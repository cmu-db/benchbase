#!/bin/bash
echo "START OF DAY"
echo "023-02-01 03:25:00========================================"


sleep 300
echo "023-02-01 03:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &


sleep 300
echo "023-02-01 03:35:00========================================"

sleep 300
echo "023-02-01 03:40:00========================================"

sleep 300
echo "023-02-01 03:45:00========================================"

sleep 300
echo "023-02-01 03:50:00========================================"

sleep 300
echo "023-02-01 03:55:00========================================"

sleep 300
echo "023-02-01 04:00:00========================================"

sleep 300
echo "023-02-01 04:05:00========================================"

sleep 300
echo "023-02-01 04:10:00========================================"

sleep 300
echo "023-02-01 04:15:00========================================"

sleep 300
echo "023-02-01 04:20:00========================================"

sleep 300
echo "023-02-01 04:25:00========================================"
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-11-28-957898.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-11-28-957898.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &


sleep 300
echo "023-02-01 04:30:00========================================"

sleep 300
echo "023-02-01 04:35:00========================================"

sleep 300
echo "023-02-01 04:40:00========================================"

sleep 300
echo "023-02-01 04:45:00========================================"

sleep 300
echo "023-02-01 04:50:00========================================"

sleep 300
echo "023-02-01 04:55:00========================================"

sleep 300
echo "023-02-01 05:00:00========================================"

sleep 300
echo "023-02-01 05:05:00========================================"

sleep 300
echo "023-02-01 05:10:00========================================"

sleep 300
echo "023-02-01 05:15:00========================================"

sleep 300
echo "023-02-01 05:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-16-35-006545.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-16-36-026578.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:25:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-21-41-073553.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-21-42-097569.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-26-45-119435.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-26-46-136455.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:35:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-31-49-164210.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-31-50-180228.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:40:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-36-55-213141.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-36-56-230158.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-36-57-241176.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:45:00========================================"

sleep 300
echo "023-02-01 05:50:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-42-01-360818.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-42-02-374840.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 05:55:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-47-07-409754.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:00:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-52-11-449841.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-15-52-12-467863.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:05:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-57-15-487891.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-15-57-16-504906.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:10:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-02-21-537187.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:15:00========================================"

timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-large.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-12-25-597029.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-12-26-614045.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-12-27-629058.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:25:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-17-31-655300.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-22-33-678411.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-22-34-694436.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:35:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-27-37-718616.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-27-38-735632.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-27-39-748652.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:40:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-32-43-775089.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-32-44-791107.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:45:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-37-47-819054.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-37-48-834074.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:50:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-42-53-872967.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-42-54-887984.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 06:55:00========================================"

sleep 300
echo "023-02-01 07:00:00========================================"

sleep 300
echo "023-02-01 07:05:00========================================"

sleep 300
echo "023-02-01 07:10:00========================================"

sleep 300
echo "023-02-01 07:15:00========================================"

sleep 300
echo "023-02-01 07:20:00========================================"

sleep 300
echo "023-02-01 07:25:00========================================"

sleep 300
echo "023-02-01 07:30:00========================================"

sleep 300
echo "023-02-01 07:35:00========================================"

sleep 300
echo "023-02-01 07:40:00========================================"

sleep 300
echo "023-02-01 07:45:00========================================"

sleep 300
echo "023-02-01 07:50:00========================================"

sleep 300
echo "023-02-01 07:55:00========================================"

sleep 300
echo "023-02-01 08:00:00========================================"

sleep 300
echo "023-02-01 08:05:00========================================"

sleep 300
echo "023-02-01 08:10:00========================================"

timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:15:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-53-01-944019.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-16-58-05-982349.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-16-58-06-997363.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:25:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-03-10-018341.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-17-03-12-051376.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-08-16-074684.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:35:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-13-18-099290.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-13-19-116310.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-17-13-20-131333.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 08:40:00========================================"

sleep 300
echo "023-02-01 08:45:00========================================"

sleep 300
echo "023-02-01 08:50:00========================================"

sleep 300
echo "023-02-01 08:55:00========================================"

sleep 300
echo "023-02-01 09:00:00========================================"

sleep 300
echo "023-02-01 09:05:00========================================"

sleep 300
echo "023-02-01 09:10:00========================================"

sleep 300
echo "023-02-01 09:15:00========================================"

sleep 300
echo "023-02-01 09:20:00========================================"

sleep 300
echo "023-02-01 09:25:00========================================"

sleep 300
echo "023-02-01 09:30:00========================================"

sleep 300
echo "023-02-01 09:35:00========================================"

sleep 300
echo "023-02-01 09:40:00========================================"

sleep 300
echo "023-02-01 09:45:00========================================"

sleep 300
echo "023-02-01 09:50:00========================================"

sleep 300
echo "023-02-01 09:55:00========================================"

sleep 300
echo "023-02-01 10:00:00========================================"

sleep 300
echo "023-02-01 10:05:00========================================"

sleep 300
echo "023-02-01 10:10:00========================================"

sleep 300
echo "023-02-01 10:15:00========================================"

sleep 300
echo "023-02-01 10:20:00========================================"

sleep 300
echo "023-02-01 10:25:00========================================"

sleep 300
echo "023-02-01 10:30:00========================================"

sleep 300
echo "023-02-01 10:35:00========================================"

sleep 300
echo "023-02-01 10:40:00========================================"

sleep 300
echo "023-02-01 10:45:00========================================"

sleep 300
echo "023-02-01 10:50:00========================================"

sleep 300
echo "023-02-01 10:55:00========================================"

sleep 300
echo "023-02-01 11:00:00========================================"

sleep 300
echo "023-02-01 11:05:00========================================"

sleep 300
echo "023-02-01 11:10:00========================================"

sleep 300
echo "023-02-01 11:15:00========================================"

sleep 300
echo "023-02-01 11:20:00========================================"

sleep 300
echo "023-02-01 11:25:00========================================"

sleep 300
echo "023-02-01 11:30:00========================================"

sleep 300
echo "023-02-01 11:35:00========================================"

sleep 300
echo "023-02-01 11:40:00========================================"

sleep 300
echo "023-02-01 11:45:00========================================"

sleep 300
echo "023-02-01 11:50:00========================================"

sleep 300
echo "023-02-01 11:55:00========================================"

sleep 300


echo "023-02-01 12:00:00========================================"


sleep 300
echo "023-02-01 12:05:00========================================"


timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
sleep 300
echo "023-02-01 12:10:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 12:15:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 12:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 12:25:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 12:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
sleep 300
echo "023-02-01 12:35:00========================================"


timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
sleep 300
echo "023-02-01 12:40:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-33-32-244808.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 12:45:00========================================"


sleep 300
echo "023-02-01 12:50:00========================================"

sleep 300
echo "023-02-01 12:55:00========================================"

sleep 300
echo "023-02-01 13:00:00========================================"

sleep 300
echo "023-02-01 13:05:00========================================"

sleep 300
echo "023-02-01 13:10:00========================================"

sleep 300
echo "023-02-01 13:15:00========================================"

sleep 300
echo "023-02-01 13:20:00========================================"

sleep 300
echo "023-02-01 13:25:00========================================"

sleep 300
echo "023-02-01 13:30:00========================================"

sleep 300
echo "023-02-01 13:35:00========================================"

sleep 300
echo "023-02-01 13:40:00========================================"

sleep 300
echo "023-02-01 13:45:00========================================"

sleep 300
echo "023-02-01 13:50:00========================================"

sleep 300
echo "023-02-01 13:55:00========================================"

sleep 300
echo "023-02-01 14:00:00========================================"

sleep 300
echo "023-02-01 14:05:00========================================"

sleep 300
echo "023-02-01 14:10:00========================================"

sleep 300
echo "023-02-01 14:15:00========================================"

sleep 300
echo "023-02-01 14:20:00========================================"

sleep 300
echo "023-02-01 14:25:00========================================"

sleep 300
echo "023-02-01 14:30:00========================================"

sleep 300
echo "023-02-01 14:35:00========================================"

sleep 300
echo "023-02-01 14:40:00========================================"

sleep 300
echo "023-02-01 14:45:00========================================"

sleep 300
echo "023-02-01 14:50:00========================================"

sleep 300
echo "023-02-01 14:55:00========================================"

sleep 300
echo "023-02-01 15:00:00========================================"

sleep 300
echo "023-02-01 15:05:00========================================"

sleep 300
echo "023-02-01 15:10:00========================================"

sleep 300
echo "023-02-01 15:15:00========================================"

sleep 300
echo "023-02-01 15:20:00========================================"

sleep 300
echo "023-02-01 15:25:00========================================"

sleep 300
echo "023-02-01 15:30:00========================================"

sleep 300
echo "023-02-01 15:35:00========================================"

sleep 300
echo "023-02-01 15:40:00========================================"

sleep 300
echo "023-02-01 15:45:00========================================"

sleep 300
echo "023-02-01 15:50:00========================================"

sleep 300
echo "023-02-01 15:55:00========================================"

sleep 300
echo "023-02-01 16:00:00========================================"

sleep 300
echo "023-02-01 16:05:00========================================"

sleep 300
echo "023-02-01 16:10:00========================================"

sleep 300
echo "023-02-01 16:15:00========================================"

sleep 300
echo "023-02-01 16:20:00========================================"

sleep 300
echo "023-02-01 16:25:00========================================"

sleep 300
echo "023-02-01 16:30:00========================================"

sleep 300
echo "023-02-01 16:35:00========================================"

sleep 300
echo "023-02-01 16:40:00========================================"

sleep 300
echo "023-02-01 16:45:00========================================"

sleep 300
echo "023-02-01 16:50:00========================================"

sleep 300
echo "023-02-01 16:55:00========================================"

sleep 300
echo "023-02-01 17:00:00========================================"

sleep 300
echo "023-02-01 17:05:00========================================"

sleep 300
echo "023-02-01 17:10:00========================================"

sleep 300
echo "023-02-01 17:15:00========================================"

sleep 300
echo "023-02-01 17:20:00========================================"

sleep 300
echo "023-02-01 17:25:00========================================"

sleep 300
echo "023-02-01 17:30:00========================================"

sleep 300
echo "023-02-01 17:35:00========================================"

sleep 300
echo "023-02-01 17:40:00========================================"

sleep 300
echo "023-02-01 17:45:00========================================"

sleep 300
echo "023-02-01 17:50:00========================================"

sleep 300
echo "023-02-01 17:55:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-09-00-511868.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:00:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-14-05-571519.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:05:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-19-10-610778.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:10:00========================================"


timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-19-10-610778.xml" --execute=true -d results/stitcher &
sleep 300
echo "023-02-01 18:15:00========================================"

timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-18-29-21-718384.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-34-24-739510.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:25:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-39-30-788305.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-18-39-31-804326.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:30:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-23-26-183729.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-17-23-26-183729.xml" --execute=true -d results/stitcher &
sleep 300

sleep 300
echo "023-02-01 18:35:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-49-42-908969.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:40:00========================================"

timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-large.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:45:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-18-59-46-962565.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-18-59-47-977587.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:50:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-04-50-998262.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpch -c "config/sqlserver/stitcher/tpch-small.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 18:55:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-09-55-037078.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-09-56-054136.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-19-09-57-070178.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 19:00:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-15-01-106782.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-19-15-02-122796.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 19:05:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-20-05-152938.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-19-20-06-168952.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 19:10:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-25-09-189451.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b ycsb -c "config/sqlserver/stitcher/2023-02-24-19-25-10-205475.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 19:15:00========================================"

sleep 300
echo "023-02-01 19:20:00========================================"

timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-35-15-252552.xml" --execute=true -d results/stitcher &
timeout 330s java -jar benchbase.jar -b tpcc -c "config/sqlserver/stitcher/2023-02-24-19-35-16-264572.xml" --execute=true -d results/stitcher &

sleep 300
echo "023-02-01 19:25:00========================================"

sleep 300
echo "023-02-01 19:30:00========================================"

sleep 300
echo "023-02-01 19:35:00========================================"

sleep 300
echo "023-02-01 19:40:00========================================"

sleep 300
echo "023-02-01 19:45:00========================================"

sleep 300
echo "023-02-01 19:50:00========================================"

sleep 300
echo "023-02-01 19:55:00========================================"

sleep 300
echo "023-02-01 20:00:00========================================"

sleep 300
echo "023-02-01 20:05:00========================================"

sleep 300
echo "023-02-01 20:10:00========================================"

sleep 300
echo "023-02-01 20:15:00========================================"

sleep 300
echo "023-02-01 20:20:00========================================"

sleep 300
echo "023-02-01 20:25:00========================================"

sleep 300
echo "023-02-01 20:30:00========================================"

sleep 300
echo "023-02-01 20:35:00========================================"

sleep 300
echo "023-02-01 20:40:00========================================"

sleep 300
echo "023-02-01 20:45:00========================================"

sleep 300
echo "023-02-01 20:50:00========================================"

sleep 300
echo "023-02-01 20:55:00========================================"

sleep 300
echo "023-02-01 21:00:00========================================"

sleep 300
echo "023-02-01 21:05:00========================================"

sleep 300
echo "023-02-01 21:10:00========================================"

sleep 300
echo "023-02-01 21:15:00========================================"

sleep 300
echo "023-02-01 21:20:00========================================"

sleep 300
echo "023-02-01 21:25:00========================================"

sleep 300
echo "023-02-01 21:30:00========================================"

sleep 300
echo "023-02-01 21:35:00========================================"

sleep 300
echo "023-02-01 21:40:00========================================"

sleep 300
echo "023-02-01 21:45:00========================================"

sleep 300
echo "023-02-01 21:50:00========================================"

sleep 300
echo "023-02-01 21:55:00========================================"

sleep 300
echo "023-02-01 22:00:00========================================"

sleep 300
echo "023-02-01 22:05:00========================================"

sleep 300
echo "023-02-01 22:10:00========================================"

sleep 300
echo "023-02-01 22:15:00========================================"

sleep 300
echo "023-02-01 22:20:00========================================"

sleep 300
echo "023-02-01 22:25:00========================================"

sleep 300
echo "023-02-01 22:30:00========================================"

sleep 300
echo "023-02-01 22:35:00========================================"

sleep 300
echo "023-02-01 22:40:00========================================"

sleep 300
echo "023-02-01 22:45:00========================================"

sleep 300
echo "023-02-01 22:50:00========================================"

sleep 300
echo "023-02-01 22:55:00========================================"

sleep 300
echo "023-02-01 23:00:00========================================"

sleep 300
echo "023-02-01 23:05:00========================================"

sleep 300
echo "023-02-01 23:10:00========================================"

sleep 300
echo "023-02-01 23:15:00========================================"

sleep 300
echo "023-02-01 23:20:00========================================"

sleep 300
echo "023-02-01 23:25:00========================================"

sleep 300
echo "023-02-01 23:30:00========================================"

sleep 300
echo "023-02-01 23:35:00========================================"

sleep 300
echo "023-02-01 23:40:00========================================"

sleep 300
echo "023-02-01 23:45:00========================================"

sleep 300
echo "023-02-01 23:50:00========================================"

sleep 300
echo "023-02-01 23:55:00========================================"

sleep 300
echo "023-02-02 00:00:00========================================"

sleep 300
echo "023-02-02 00:05:00========================================"

sleep 300
echo "023-02-02 00:10:00========================================"

sleep 300
echo "023-02-02 00:15:00========================================"

sleep 300
echo "023-02-02 00:20:00========================================"

sleep 300
echo "023-02-02 00:25:00========================================"

sleep 300
echo "023-02-02 00:30:00========================================"

sleep 300
echo "023-02-02 00:35:00========================================"

sleep 300
echo "023-02-02 00:40:00========================================"

sleep 300
echo "023-02-02 00:45:00========================================"

sleep 300
echo "023-02-02 00:50:00========================================"

sleep 300
echo "023-02-02 00:55:00========================================"

sleep 300
echo "023-02-02 01:00:00========================================"

sleep 300
echo "023-02-02 01:05:00========================================"

sleep 300
echo "023-02-02 01:10:00========================================"

sleep 300
echo "023-02-02 01:15:00========================================"

sleep 300
echo "023-02-02 01:20:00========================================"

sleep 300
echo "023-02-02 01:25:00========================================"

sleep 300
echo "023-02-02 01:30:00========================================"

sleep 300
echo "023-02-02 01:35:00========================================"

sleep 300
echo "023-02-02 01:40:00========================================"

sleep 300
echo "023-02-02 01:45:00========================================"

sleep 300
echo "023-02-02 01:50:00========================================"

sleep 300
echo "023-02-02 01:55:00========================================"

sleep 300
echo "023-02-02 02:00:00========================================"

sleep 300
echo "023-02-02 02:05:00========================================"

sleep 300
echo "023-02-02 02:10:00========================================"

sleep 300
echo "023-02-02 02:15:00========================================"

sleep 300
echo "023-02-02 02:20:00========================================"

sleep 300
echo "023-02-02 02:25:00========================================"

sleep 300
echo "023-02-02 02:30:00========================================"

sleep 300
echo "023-02-02 02:35:00========================================"

sleep 300
echo "023-02-02 02:40:00========================================"

sleep 300
echo "023-02-02 02:45:00========================================"

sleep 300
echo "023-02-02 02:50:00========================================"

sleep 300
echo "023-02-02 02:55:00========================================"

sleep 300
echo "023-02-02 03:00:00========================================"

sleep 300
echo "023-02-02 03:05:00========================================"

sleep 300
echo "023-02-02 03:10:00========================================"

sleep 300
echo "023-02-02 03:15:00========================================"

sleep 300
echo "023-02-02 03:20:00========================================"

sleep 300
echo "023-02-02 03:25:00========================================"

echo "END OF DAY"
