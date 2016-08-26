# Hbase Export Snapshot
A custom asynchronous oozie action to export Hbase snapshots.

## Install
In order to manually install this custom action on a
[BACH](https://github.com/bloomberg/chef-bach) cluster:
- Install maven 3.3 or higher
- Build the project into a jar:
```
$ mvn package
```
- This will create a jar containing the class binaries. Un-comment the "maven-shade-plugin" portion of the pom.xml file to create an uber-jar containing all of the necessary dependencies.

On the host where the oozie server is installed:
- Copy the class jar from `target/hbaseoozie-1.0-SNAPSHOT.jar` to `/usr/hdp/current/oozie-server/libext/`
- Copy the hbase jars to `/usr/hdp/current/oozie-server/libext/`
  * `hbase-hadoop2-compat-1.1.2.2.3.4.0-3485.jar`
  * `htrace-core-3.1.0-incubating.jar`
  * `hbase-client-1.1.2.2.3.4.0-3485.jar`
  * `hbase-common-1.1.2.2.3.4.0-3485.jar`
  * `hbase-protocol-1.1.2.2.3.4.0-3485.jar`
  * `hbase-server-1.1.2.2.3.4.0-3485.jar`
  * `netty-3.2.4.Final.jar`
  * `netty-all-4.0.23.Final.jar`
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
         org.apache.oozie.action.hadoop.DistcpActionExecutor
         org.apache.oozie.action.hadoop.DistcpActionExecutor,
+        org.apache.oozie.action.hadoop.HbaseExportSnapshotActionExecutor
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
         distcp-action-0.1.xsd
         distcp-action-0.1.xsd,
+        hbase-export-snapshot-action-0.1.xsd
       </value>
     </property>
```
- Rebuild the oozie war and restart the oozie service:
```
$ /usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war
$ sudo service oozie restart
```

## Run
On the the host where you are running the job from:
- Define the oozie URL (e.g. `export OOZIE_URL="http://f-bcpc-vm2.bcpc.example.com:11000/oozie"`)
- Create the action root directory where the workflow belongs. Put the class jar, dependencies jar and hbase jars in the `/lib` directory next to the workflow (alternatively, you can utilize the [oozie shared library](http://blog.cloudera.com/blog/2014/05/how-to-use-the-sharelib-in-apache-oozie-cdh-5/)): 
- Put the workflow.xml in the action root next to the `/lib` directory
- Put the action root directory with the workflow and `/lib` directory on HDFS (this may require creating /user/ubuntu directory and chown to ubuntu)
