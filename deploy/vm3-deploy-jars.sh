# deploy-jars.sh

hdfs dfs -put -f hbaseoozie-1.0-SNAPSHOT.jar  hbaseoozie/hbase-export-snapshot/lib/hbaseoozie-1.0-SNAPSHOT.jar
echo "Kicking off oozie job:"
oozie job -config ~/job.properties -run
