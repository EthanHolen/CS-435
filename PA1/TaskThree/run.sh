$HADOOP_HOME/bin/hadoop fs -rm -r /pa1result3

$HADOOP_HOME/bin/hadoop jar ~/cs435/PA1/TaskThree/TaskThree.jar TaskThree /PreprocessedDataset.txt /pa1result3


echo "RESULT 1 Files: "

$HADOOP_HOME/bin/hadoop fs -ls /pa1result3

echo "RESULT 1 answer: "

$HADOOP_HOME/bin/hadoop fs -cat /pa1result3/part-r-00000