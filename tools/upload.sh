EXP_CONF=$1
SAMPLE_RESULT=$2
SUMMARY=$3
CODE=$4
URL=$5
DB_TYPE=$6
DB_CONF="/tmp/$(basename $0).$RANDOM.txt"
TEMP_FILE="/tmp/$(basename $0).$RANDOM.txt"

echo $TEMP_FILE

$(dirname $0)/collector.sh ${DB_TYPE,,} $DB_CONF

wc -l < $DB_CONF > $TEMP_FILE
wc -l < $EXP_CONF >> $TEMP_FILE
wc -l < $SAMPLE_RESULT >> $TEMP_FILE
wc -l < $SUMMARY >> $TEMP_FILE

cat $DB_CONF >> $TEMP_FILE
cat $EXP_CONF >> $TEMP_FILE
cat $SAMPLE_RESULT >> $TEMP_FILE
cat $SUMMARY >> $TEMP_FILE

echo Uploading to $URL with code $CODE

curl --form upload_code=$CODE --form "data=@"$TEMP_FILE $URL

rm $DB_CONF
rm $TEMP_FILE
