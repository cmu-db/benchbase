PROFILE=$1
BENCHMARK_NAME=$2
java -jar benchbase.jar -b ${BENCHMARK_NAME} -c config/${PROFILE}/sample_${BENCHMARK_NAME}_config.xml --create=true --load=true --execute=true