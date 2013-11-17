EXP_CONF=$1
SAMPLE_RESULT=$2
SUMMARY=$3
DB_CONF=$4
CODE=$5
URL=$6
TEMP_FILE="/tmp/$(basename $0).$RANDOM.txt"

echo $TEMP_FILE

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

rm $TEMP_FILE
