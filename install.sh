s3cmd sync s3://jxtab1 /home/thiago/s3/jxtab1/
mvn install
cp target/hadoop-examples-0.0.1-jar-with-dependencies.jar ~/s3/jxtab1/samples/
cp target/hadoop-examples-0.0.1.jar ~/s3/jxtab1/samples/
s3cmd sync --delete-removed /home/thiago/s3/jxtab1/ s3://jxtab1/

