$HADOOP_HOME/bin/hadoop fs -rm -r /pa1result1

$HADOOP_HOME/bin/hadoop jar ~/cs435/PA1/TaskOne/TaskOne.jar TaskOne /PreprocessedDataset.txt /pa1result1


echo "RESULT 1 Files: "

$HADOOP_HOME/bin/hadoop fs -ls /pa1result1

echo "RESULT 1 answer: "

$HADOOP_HOME/bin/hadoop fs -cat /pa1result1/part-r-00000