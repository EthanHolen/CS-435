rm *.class
rm TaskOne.jar

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TaskOne.java

jar cf TaskOne.jar TaskOne*.class