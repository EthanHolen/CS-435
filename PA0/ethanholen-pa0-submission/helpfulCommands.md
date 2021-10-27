
## list contents of hadoop root

$HADOOP_HOME/bin/hadoop fs -ls /

## start hdfs
$HADOOP_HOME/sbin/start-dfs.sh


## stop hdfs

$HADOOP_HOME/sbin/stop-dfs.sh


## start yarn

$HADOOP_HOME/sbin/start-yarn.sh


## stop yarn

 $HADOOP_HOME/sbin/stop-yarn.sh



## run command

$HADOOP_HOME/bin/hadoop jar <path_to_your.jar> <yourMainClass> <argument_1>...<argument_N>
$HADOOP_HOME/bin/hadoop jar <path_to_your.jar> <yourMainClass> <argument_1>...<argument_N>


ex.

$HADOOP_HOME/bin/hadoop jar ~/example/DistinctNodes.jar DistinctNodes /gplus_combined.txt /result-folder



