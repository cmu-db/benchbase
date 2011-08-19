BASEDIR=/Users/Djellel/Documents/workspace
echo -ne "./bin"
for i in `ls ./lib/*.jar`
do
echo -ne ":$i"
done

