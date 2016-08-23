package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.action.hadoop.LauncherSecurityManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.snapshot.ExportSnapshot;

import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.util.Collection;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class HbaseExportSnapshotMain extends LauncherMain {

    private static final String NODENAME = "hbase-export-snapshot";

    protected XLog LOG = XLog.getLog(getClass());

    public static void main(String[] args) throws Exception {
        run(HbaseExportSnapshotMain.class, args);
    }

    protected void run(String[] args) throws Exception {
        LOG.debug("Inside launcher runner");
        System.out.println();
        System.out.println("Oozie Hbase Export Snapshot action configuration");
        System.out
                .println("=============================================");
        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));

        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") == null) {
            throw new Exception("HADOOP_TOKEN_FILE_LOCATION not set");
        }

        // propagate delegation related props from launcher job to MR job
        if (getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            actionConf.set("mapreduce.job.credentials.binary", getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION"));
        } else {
            throw new RuntimeException("Hadoop token file location is null");
        }

        try {
            File inFile = new File(System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            FileInputStream fis = new FileInputStream(inFile);
            DataInputStream dis = new DataInputStream(fis);
            Credentials creds = new Credentials();
            creds.readTokenStorageStream(dis);
            System.out.println("Storing tokens: " + creds.numberOfTokens());
            Collection<Token<? extends TokenIdentifier>> toks = creds.getAllTokens();
            for (Token tok : toks) {
                System.out.println("Token kind: " + tok.getKind().toString());
                System.out.println("Token service: " + tok.getService().toString());
            }
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }

        /*
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            actionConf.set("mapreduce.job.credentials.binary",
                    System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        } else {
            throw new RuntimeException("Hadoop token file location is null");
        }*/

        // SNAPSHOT_NAME
        String snapshotName = actionConf.get(HbaseExportSnapshotActionExecutor.SNAPSHOT_NAME);
        if (snapshotName == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + HbaseExportSnapshotActionExecutor.SNAPSHOT_NAME + " property");
        }
        // DESTINATION_URI
        String destinationUri = actionConf.get(HbaseExportSnapshotActionExecutor.DESTINATION_URI);
        if (destinationUri == null) {
            throw new RuntimeException("Action Configuration does not have "
                    + HbaseExportSnapshotActionExecutor.DESTINATION_URI + " property");
        }

        LOG.debug("Starting " + NODENAME + " for snapshot " + snapshotName);

        String[] params = { "-snapshot", snapshotName, "-copy-to", destinationUri };

        try {
            ToolRunner.run(actionConf, new ExportSnapshot(), params);
        } catch (Exception ex) {
            //convertException(e);
            throw ex;
        }
    }
}