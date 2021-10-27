rm *.class
rm GeodesicPath.jar

$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main GeodesicPath.java

jar cf GeodesicPath.jar GeodesicPath*.class