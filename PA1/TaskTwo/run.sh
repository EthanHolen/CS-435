$HADOOP_HOME/bin/hadoop fs -rm -r /pa1result2

$HADOOP_HOME/bin/hadoop jar ~/cs435/PA1/TaskTwo/TaskTwo.jar TaskTwo /PreprocessedDataset.txt /pa1result2/indegreetemp /pa1result2/outdegreetemp /pa1result2/indegreeanswer /pa1result2/outdegreeanswer

$HADOOP_HOME/bin/hadoop fs -ls /pa1result2

# echo ""
# echo "indegree: "
# echo ""
# $HADOOP_HOME/bin/hadoop fs -cat /pa1result2/indegreetemp/part-r-00000
# echo ""
# echo "outdegree: "
# echo ""
# $HADOOP_HOME/bin/hadoop fs -cat /pa1result2/outdegreetemp/part-r-00000


echo ""
echo "in-answer: "
echo ""
$HADOOP_HOME/bin/hadoop fs -cat /pa1result2/indegreeanswer/part-r-00000
echo ""
echo "out-answer: "
echo ""
$HADOOP_HOME/bin/hadoop fs -cat /pa1result2/outdegreeanswer/part-r-00000