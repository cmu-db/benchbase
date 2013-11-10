case "$1" in
    'mysql' )
        mysqladmin variables | grep "|.*|.*|" | sed s/"|\s*\(\S*\)\s*|\s*\(.*\)\s*|"/"\1:\2"/ | sed s/"\s*$"// > $2
        ;;
    'postgres' )
        psql -c "SHOW ALL;" -U postgres | grep ".*|.*|.*" | sed s/"\s*\(\S*\)\s*|\s*\(\S*\)\s*|.*"/"\1:\2"/ > $2
        ;;
esac
