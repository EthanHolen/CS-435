$HADOOP_HOME/bin/hadoop fs -rm -r /pa2result1

$HADOOP_HOME/bin/hadoop jar ~/cs435/PA2/TaskOne/TaskOne.jar TaskOne /friends.txt /pa2result1/edges /pa2result1/vertices



echo "PA2T1: Number of edges"

$HADOOP_HOME/bin/hadoop fs -cat /pa2result1/edges/part-r-00000

echo "PA2T1: Number of vertices"

$HADOOP_HOME/bin/hadoop fs -cat /pa2result1/vertices/part-r-00000