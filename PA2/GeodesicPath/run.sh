hadoop=$HADOOP_HOME/bin/hadoop

MAINFOLDER=/GeodesicPathResults

JARPATH=~/cs435/PA2/GeodesicPath/GeodesicPath.jar

# INPUT=/short-friends.txt
INPUT=/sample-input.txt

arg1=$MAINFOLDER/adjacencyList
arg2=$MAINFOLDER/lengthOne
arg3=$MAINFOLDER/lengthTwo
arg4=$MAINFOLDER/lengthThree


$hadoop fs -rm -r $MAINFOLDER

$HADOOP_HOME/bin/hadoop jar $JARPATH GeodesicPath $INPUT $arg1 $arg2 $arg3 $arg4


echo "PA2: Folders "

$HADOOP_HOME/bin/hadoop fs -ls $MAINFOLDER

echo "PA2: Adjacency List "

$HADOOP_HOME/bin/hadoop fs -cat $MAINFOLDER/adjacencyList/part-r-00000

echo "PA2: LengthOne "
$HADOOP_HOME/bin/hadoop fs -cat $MAINFOLDER/lengthOne/part-r-00000

echo "PA2: LengthTwo "
$HADOOP_HOME/bin/hadoop fs -cat $MAINFOLDER/lengthTwo/part-r-00000