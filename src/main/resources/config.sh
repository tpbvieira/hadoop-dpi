sudo -s
export JAVA_HOME=/usr/lib/jvm/java-6-sun-1.6.0.26
export HADOOP_HOME=/home/hadoop
/home/hadoop/bin/hadoop fs -get s3n://jxta/*.so file:///usr/lib/
ldconfig

