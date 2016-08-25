# deploy-jars.sh

sudo cp ~/hbaseoozie-1.0-SNAPSHOT.jar /usr/hdp/current/oozie-server/libext/hbaseoozie-1.0-SNAPSHOT.jar

sudo /usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war

sudo service oozie restart
