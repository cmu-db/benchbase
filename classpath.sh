echo -ne "build"
for i in `ls lib/*.jar`
do
echo -ne ":$i"
done
for i in `ls lib/hibernate/*.jar`
do
echo -ne ":$i"
done

