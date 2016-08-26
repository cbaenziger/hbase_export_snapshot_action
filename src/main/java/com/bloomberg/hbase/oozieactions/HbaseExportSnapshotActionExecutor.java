package org.apache.oozie.action.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.UnknownHostException;
//import java.nio.file.FileSystem;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.security.token.TokenIdentifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Collection;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.Credentials;

import org.apache.hadoop.yarn.conf.*;

import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.CredentialsProvider;
import org.apache.oozie.action.hadoop.CredentialsProperties;
import org.apache.hadoop.security.Credentials;
import org.apache.oozie.action.hadoop.HbaseCredentials;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.LogUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.util.Records;

import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapred.JobID;
import org.apache.oozie.WorkflowActionBean;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobClient;


public class HbaseExportSnapshotActionExecutor extends JavaActionExecutor {

    private static final String HBASE_EXPORT_SNAPSHOT_MAIN_CLASS_NAME =
            "org.apache.oozie.action.hadoop.HbaseExportSnapshotMain";

    public static final String SNAPSHOT_NAME = "oozie.hbase-export-snapshot.snapshot.name";
    public static final String DESTINATION_URI = "oozie.hbase-export-snapshot.destination.uri";

    private static final String HBASE_USER = "hbase";
    private static final String HBASE_DIR = "hbase.rootdir";
    private static final String HDFS_USER = "hdfs";
    private static final String HADOOP_USER = "user.name";
    private static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    private static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    private static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private static final String HADOOP_NAME_NODE = "fs.default.name";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";

    protected XLog LOG = XLog.getLog(getClass());

    public HbaseExportSnapshotActionExecutor() {
        super("hbase-export-snapshot");
    }

    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        try {
            classes.add(Class.forName(HBASE_EXPORT_SNAPSHOT_MAIN_CLASS_NAME));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS,
                HBASE_EXPORT_SNAPSHOT_MAIN_CLASS_NAME);
    }

    @Override
    public void initActionType() {
        super.initActionType();
        registerError(UnknownHostException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES001");
        registerError(AccessControlException.class.getName(), ActionExecutorException.ErrorType.NON_TRANSIENT,
                "JA002");
        registerError(DiskChecker.DiskOutOfSpaceException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES003");
        registerError(org.apache.hadoop.hdfs.protocol.QuotaExceededException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES004");
        registerError(org.apache.hadoop.hdfs.server.namenode.SafeModeException.class.getName(),
                ActionExecutorException.ErrorType.NON_TRANSIENT, "HES005");
        registerError(ConnectException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "  HES006");
        registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES007");
        registerError(FileNotFoundException.class.getName(), ActionExecutorException.ErrorType.ERROR, "HES008");
        registerError(IOException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "HES009");
    }

    private static void checkForDisallowedProps(XConfiguration conf, String confName) throws ActionExecutorException {
        for (String prop : DISALLOWED_PROPERTIES) {
            if (conf.get(prop) != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                        "Property [{0}] not allowed in action [{1}] configuration", prop, confName);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context,
                                  Element actionXml, Path appPath) throws ActionExecutorException {
        Namespace ns = actionXml.getNamespace();
        String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();

        //actionConf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);

        super.setupActionConf(actionConf, context, actionXml, appPath);

        LOG.debug("Setting up action conf");

        String strConf = null;
        Element e = actionXml.getChild("configuration", ns);
        if (e != null) {
            strConf = XmlUtils.prettyPrint(e).toString();
        }

        XConfiguration inlineConf = null;
        try {
            inlineConf = new XConfiguration(new StringReader(strConf));
        } catch (IOException ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                    "XConfiguration StringReader IOException");
        }
        try {
            checkForDisallowedProps(inlineConf, "inline configuration");
        } catch (ActionExecutorException ex) {
            //convertException(e);
            throw ex;
        }
        XConfiguration.copy(inlineConf, actionConf);

        actionConf.set(HADOOP_USER, HBASE_USER);
        actionConf.set(HADOOP_JOB_TRACKER, jobTracker);
        actionConf.set(HADOOP_JOB_TRACKER_2, jobTracker);
        actionConf.set(HADOOP_YARN_RM, jobTracker);
        actionConf.set(HADOOP_NAME_NODE, nameNode);
        actionConf.set(HBASE_DIR, "/hbase");
        actionConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");
        actionConf.set("oozie.action.dir.path", "hdfs://f-bcpc-vm1.bcpc.example.com:8020/tmp/hbase-export-snapshot");

        // Set job name
        String jobName = actionConf.get(HADOOP_JOB_NAME);
        String actionType = getType();
        String appName = context.getWorkflow().getAppName();
        String actionName = "hbase-export-snapshot";
        String workflowId = context.getWorkflow().getId();
        if (jobName == null || jobName.isEmpty()) {
            jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}",
                    actionType, appName, actionName, workflowId);
            actionConf.set(HADOOP_JOB_NAME, jobName);
        }

        // Set callback
        if (actionConf.get("job.end.notification.url") != null) {
            LOG.warn("Overriding the action job end notification URI");
        }
        actionConf.set("job.end.notification.url", context.getCallbackUrl("$jobStatus"));

        // Set snapshot name and destination URI
        actionConf.set(SNAPSHOT_NAME, actionXml.getChild("snapshot-name", ns).getTextTrim());
        actionConf.set(DESTINATION_URI, actionXml.getChild("destination-uri", ns).getTextTrim());

        context.setStartData("-", "-", "-");

        return actionConf;
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LogUtils.setLogInfo(action);

        // Create proxy user
        UserGroupInformation loggedUserUgi;
        try {
            loggedUserUgi = UserGroupInformation.getLoginUser();
        } catch (Exception ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                    "Error getting login user UGI");
        }
        String loggedUserName = loggedUserUgi.getShortUserName();
        UserGroupInformation proxyUserUgi =  UserGroupInformation.createProxyUser("hbase", loggedUserUgi);


        final Context contextFinal = context;
        final WorkflowAction  actionFinal = action;

        try {
            LOG.debug("Action Dir is ready. Submitting the action ");
            submitLauncher(null, contextFinal, actionFinal);

            LOG.debug("Action submit completed. Performing check ");
            check(contextFinal, actionFinal);

        } catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private boolean needInjectCredentials() {
        boolean methodExists = true;

        Class klass;
        try {
            klass = Class.forName("org.apache.hadoop.mapred.JobConf");
            klass.getMethod("getCredentials");
        }
        catch (ClassNotFoundException ex) {
            methodExists = false;
        }
        catch (NoSuchMethodException ex) {
            methodExists = false;
        }

        return methodExists;
    }

    @Override
    public void submitLauncher(FileSystem actionFs, Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Path appPathRoot = new Path(context.getWorkflow().getAppPath());

            Element actionXml = XmlUtils.parseXml(action.getConf());

            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);

            //JobConf actionConf = new JobConf();
            Configuration actionConf = loadHadoopDefaultResources(context, actionXml);

            setupActionConf(actionConf, context, actionXml, appPathRoot);

            String jobName = actionConf.get(HADOOP_JOB_NAME);
            if (jobName == null || jobName.isEmpty()) {
                jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}",
                        getType(), context.getWorkflow().getAppName(),
                        action.getName(), context.getWorkflow().getId());
                actionConf.set(HADOOP_JOB_NAME, jobName);
            }

            actionConf.setBoolean("oozie.HadoopAccessorService.created", true);
            URI uri = context.getAppFileSystem().getUri();
            actionFs = has.createFileSystem("hbase", uri, actionConf);

            // app path could be a file
            if (actionFs.isFile(appPathRoot)) {
                appPathRoot = appPathRoot.getParent();
            }

            LOG.debug("Setting LibFilesArchives ");
            setLibFilesArchives(context, actionXml, appPathRoot, actionConf);

            injectActionCallback(context, actionConf);

            if(actionConf.get(ACL_MODIFY_JOB) == null || actionConf.get(ACL_MODIFY_JOB).trim().equals("")) {
                // ONLY in the case where user has not given the
                // modify-job ACL specifically
                if (context.getWorkflow().getAcl() != null) {
                    // setting the group owning the Oozie job to allow anybody in that
                    // group to modify the jobs.
                    actionConf.set(ACL_MODIFY_JOB, context.getWorkflow().getAcl());
                }
            }

            // Setting the credential properties in launcher conf
            JobConf credentialsConf = null;
            HashMap<String, CredentialsProperties> credentialsProperties = setCredentialPropertyToActionConf(context,
                    action, actionConf);
            if (credentialsProperties != null) {

                // Adding if action need to set more credential tokens
                credentialsConf = new JobConf(false);
                XConfiguration.copy(actionConf, credentialsConf);
                //setCredentialTokens(credentialsConf, context, action, credentialsProperties);

                // insert conf to action conf from credentialsConf
                for (Entry<String, String> entry : credentialsConf) {
                    if (actionConf.get(entry.getKey()) == null) {
                        actionConf.set(entry.getKey(), entry.getValue());
                    }
                }
            }

            JobConf launcherJobConf = createLauncherConf(actionFs, context, action, actionXml, actionConf);

            launcherJobConf.setUser("hbase");
            //launcherJobConf.setStrings("user.name", "hbase");

            LOG.debug("Creating Job Client for action " + action.getId());
            //jobClient = createJobClient(context, launcherJobConf);
            jobClient = Services.get().get(HadoopAccessorService.class).createJobClient("hbase", launcherJobConf);
            String launcherId = LauncherMapperHelper.getRecoveryId(launcherJobConf, context.getActionDir(), context
                    .getRecoveryId());
            boolean alreadyRunning = launcherId != null;
            RunningJob runningJob;

            // if user-retry is on, always submit new launcher
            boolean isUserRetry = ((WorkflowActionBean)action).isUserRetry();

            if (alreadyRunning && !isUserRetry) {
                runningJob = jobClient.getJob(JobID.forName(launcherId));
                if (runningJob == null) {
                    String jobTracker = launcherJobConf.get(HADOOP_JOB_TRACKER);
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                            "unknown job [{0}@{1}], cannot recover", launcherId, jobTracker);
                }
            }
            else {
                LOG.debug("Submitting the job through Job Client for action " + action.getId());

                // setting up propagation of the delegation token.
                has = Services.get().get(HadoopAccessorService.class);
                Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(has
                        .getMRDelegationTokenRenewer(launcherJobConf));
                launcherJobConf.getCredentials().addToken(HadoopAccessorService.MR_TOKEN_ALIAS, mrdt);

                // insert credentials tokens to launcher job conf if needed
                if (needInjectCredentials() && credentialsConf != null) {
                    for (Token<? extends TokenIdentifier> tk : credentialsConf.getCredentials().getAllTokens()) {
                        Text fauxAlias = new Text(tk.getKind() + "_" + tk.getService());
                        LOG.debug("ADDING TOKEN: " + fauxAlias);
                        launcherJobConf.getCredentials().addToken(fauxAlias, tk);
                    }
                    if (credentialsConf.getCredentials().numberOfSecretKeys() > 0) {
                        for (Entry<String, CredentialsProperties> entry : credentialsProperties.entrySet()) {
                            CredentialsProperties credProps = entry.getValue();
                            if (credProps != null) {
                                Text credName = new Text(credProps.getName());
                                byte[] secKey = credentialsConf.getCredentials().getSecretKey(credName);
                                if (secKey != null) {
                                    LOG.debug("ADDING CREDENTIAL: " + credProps.getName());
                                    launcherJobConf.getCredentials().addSecretKey(credName, secKey);
                                }
                            }
                        }
                    }
                }
                else {
                    LOG.info("No need to inject credentials.");
                }

                launcherJobConf.setStrings("oozie.action.dir.path", "/tmp");
                launcherJobConf.setStrings("mapreduce.output.fileoutputformat.outputdir",
                        "hdfs://f-bcpc-vm1.bcpc.example.com:8020/tmp/hbase-export-snapshot/output");
                launcherJobConf.setStrings("mapred.output.dir",
                        "hdfs://f-bcpc-vm1.bcpc.example.com:8020/tmp/hbase-export-snapshot/output");


                FileSystem fsys = FileSystem.get(launcherJobConf);
                FSDataOutputStream out = fsys.create(new Path("/tmp/" + action.getId()));
                launcherJobConf.writeXml(out);
                out.close();

                runningJob = jobClient.submitJob(launcherJobConf);
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                            "Error submitting launcher for action [{0}]", action.getId());
                }
                launcherId = runningJob.getID().toString();
                LOG.debug("After submission get the launcherId " + launcherId);
            }

            String jobTracker = launcherJobConf.get(HADOOP_JOB_TRACKER);
            String consoleUrl = runningJob.getTrackingURL();
            context.setStartData(launcherId, jobTracker, consoleUrl);
        }
        catch (Exception ex) {
            exception = true;
            throw convertException(ex);
        }
        finally {
            if (jobClient != null) {
                try {
                    jobClient.close();
                }
                catch (Exception e) {
                    if (exception) {
                        LOG.error("JobClient error: ", e);
                    }
                    else {
                        throw convertException(e);
                    }
                }
            }
        }
    }

    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "hbase-export-snapshot";
    }
}