package de.l3s.gossen.yarn;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        if (args.length < 1) {
            System.err.println("Usage: java " + Client.class.getName() +" hdfsJarPath");
            return;
        }

        Configuration conf = new Configuration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();

        YarnClientApplication app = client.createApplication();
        ApplicationSubmissionContext appContext = createAM(conf, app.getApplicationSubmissionContext(), args[0]);
        ApplicationId applicationId = appContext.getApplicationId();
        LOG.info("Submitting app {}", applicationId);
        ApplicationId appId = client.submitApplication(appContext);

        while (true) {
            ApplicationReport report = client.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            LOG.info("State={}, Progress={}", state, report.getProgress());
            if (state == YarnApplicationState.FINISHED || state == YarnApplicationState.FAILED
                    || state == YarnApplicationState.KILLED) {
                break;
            }
            TimeUnit.SECONDS.sleep(10);
        }
        client.stop();
    }

    private static ApplicationSubmissionContext createAM(Configuration conf, ApplicationSubmissionContext appContext,
            String jarPath) throws IOException {
        appContext.setApplicationName("archive-crawler");

        Resource resource = Records.newRecord(Resource.class);
        resource.setMemory(2 * 1024);
        resource.setVirtualCores(1);
        appContext.setResource(resource);

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setLocalResources(getLocalResources(conf, new Path(jarPath)));
        amContainer.setEnvironment(getAMEnv());
        amContainer.setCommands(getAMCommands());
        appContext.setAMContainerSpec(amContainer);
        return appContext;
    }

    private static Map<String, String> getAMEnv() {
        return Collections.singletonMap("CLASSPATH", "$CLASSPATH:./*:");
    }

    private static List<String> getAMCommands() {
        String command = "${JAVA_HOME}" + "/bin/java -Xmx1G" + " de.l3s.gossen.yarn.AppMaster" +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

        return Collections.singletonList(command);
    }

    private static Map<String, LocalResource> getLocalResources(Configuration conf, Path jarPath) throws IOException {
        FileSystem fs = jarPath.getFileSystem(conf);
        LOG.info("Got FS {} for path {}", fs, jarPath);
        FileStatus jarStatus = fs.getFileStatus(jarPath);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        amJarRsrc.setTimestamp(jarStatus.getModificationTime());
        amJarRsrc.setSize(jarStatus.getLen());
        return Collections.singletonMap("AppMaster.jar", amJarRsrc);
    }

}
