mysql-proxy --proxy-backend-addresses=127.0.0.1:3400 --admin-username root --admin-password "" --admin-lua-script ../lib/mysql-oxy/lua/admin.lua --proxy-lua-script=`pwd`"/mysqlproxylua/carlo.lua" --lua-path /usr/share/lua/5.1/?.lua --lua-cpath /usr/lib/lua/5.1/?.so &

