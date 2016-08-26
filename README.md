# Hbase Export Snapshot
A custom asynchronous oozie action to export Hbase snapshots.

## Install
In order to manually install this custom action on a
[BACH](https://github.com/bloomberg/chef-bach) cluster:

Install maven 3.3 or higher
```
wget http://mirrors.sonic.net/apache/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz
tar -zxf apache-maven-3.3.3-bin.tar.gz
sudo cp -R apache-maven-3.3.3 /usr/local
sudo ln -s /usr/local/apache-maven-3.3.3/bin/mvn /usr/bin/mvn
mvn -version
```


Building the code:
- Install maven 3.3 or higher
- Build the project into a jar:
```
$ mvn package
```
- This will create a jar containing the class binaries. Un-comment the "maven-shade-plugin" portion of the pom.xml file to create an uber-jar containing all of the necessary dependencies.

On the node where the oozie server is installed:
- Copy the class jar from `target/hbaseoozie-1.0-SNAPSHOT.jar` to /usr/hdp/current/oozie-server/libext/ 
- Copy the hbase jars to /usr/hdp/current/oozie-server/libext/
- If you care about logging, you should add the following line to the end of `/etc/oozie/conf/oozie-log4j.properties`:
```
log4j.logger.com.bloomberg.hbase.oozieactions=ALL, oozie
```
- Edit the `/etc/oozie/conf/oozie-site.xml` file adding the following lines:
```
    <property>
       <name>oozie.service.ActionService.executor.ext.classes</name>
       <value>
         org.apache.oozie.action.email.EmailActionExecutor,
         org.apache.oozie.action.hadoop.HiveActionExecutor,
         org.apache.oozie.action.hadoop.ShellActionExecutor,
         org.apache.oozie.action.hadoop.SqoopActionExecutor,
-        org.apache.oozie.action.hadoop.DistcpActionExecutor
+        org.apache.oozie.action.hadoop.DistcpActionExecutor,
+        com.bloomberg.hbase.oozieactions.HbaseExportSnapshotActionExecutor,
+        com.bloomberg.hbase.oozieactions.HbaseImportSnapshotActionExecutor
       </value>
     </property>

     <property>
       <name>oozie.service.SchemaService.wf.ext.schemas</name>
       <value>
         shell-action-0.1.xsd,
         shell-action-0.2.xsd,
         shell-action-0.3.xsd,
         email-action-0.1.xsd,
         hive-action-0.2.xsd,
         hive-action-0.3.xsd,
         hive-action-0.4.xsd,
         hive-action-0.5.xsd,
         sqoop-action-0.2.xsd,
         sqoop-action-0.3.xsd,
         ssh-action-0.1.xsd,
         ssh-action-0.2.xsd,
-        distcp-action-0.1.xsd
+        distcp-action-0.1.xsd,
+        hbase-export-snapshot-action-0.1.xsd,
+        hbase-import-snapshot-action-0.1.xsd
       </value>
     </property>
```
- Rebuild the oozie war and restart the oozie service:
```
$ /usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war
$ sudo service oozie restart
```

On the the host where you are running the job from:
- Define the oozie URL (e.g. `export OOZIE_URL="http://f-bcpc-vm2.bcpc.example.com:11000/oozie"`)
- Create the action root directory where the workflow belongs. Put the class jar, dependencies jar and hbase jars in the /lib directory next to the workflow
- Create the workflow.xml and put it in the action root next to the /lib directory
- Put the action root directory with the workflow and /lib directory on HDFS (this may require creating /user/ubuntu directory and chown to ubuntu)
