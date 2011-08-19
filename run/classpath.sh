echo -ne "../build/classes"
for i in `ls ../lib/*.jar`
do
echo -ne ":$i"
done

