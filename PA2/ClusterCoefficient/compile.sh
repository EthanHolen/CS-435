rm *.class
rm ClusterCoefficient.jar

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ClusterCoefficient.java

jar cf ClusterCoefficient.jar ClusterCoefficient*.class