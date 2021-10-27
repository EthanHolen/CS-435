$HADOOP_HOME/bin/hadoop fs -rm -r /ClusterCoefficientResults

$HADOOP_HOME/bin/hadoop jar ~/cs435/PA2/ClusterCoefficient/ClusterCoefficient.jar ClusterCoefficient /friends.txt /ClusterCoefficientResults/adjacencyList /ClusterCoefficientResults/connectedTriples /ClusterCoefficientResults/triangles /ClusterCoefficientResults/triplesCount /ClusterCoefficientResults/trianglesCount


# echo "PA2: Result 1 Files: "

# $HADOOP_HOME/bin/hadoop fs -ls /ClusterCoefficientResults/adjacencyList

# echo "PA2: Adjacency List "

# $HADOOP_HOME/bin/hadoop fs -cat /ClusterCoefficientResults/adjacencyList/part-r-00000

# echo "PA2: connectedTriples "
# $HADOOP_HOME/bin/hadoop fs -cat /ClusterCoefficientResults/connectedTriples/part-r-00000

# echo "PA2: Triangles "
# $HADOOP_HOME/bin/hadoop fs -cat /ClusterCoefficientResults/triangles/part-r-00000



echo "PA2: connectedTriples count"
$HADOOP_HOME/bin/hadoop fs -cat /ClusterCoefficientResults/triplesCount/part-r-00000

echo "PA2: Triangles count X 3"
$HADOOP_HOME/bin/hadoop fs -cat /ClusterCoefficientResults/trianglesCount/part-r-00000