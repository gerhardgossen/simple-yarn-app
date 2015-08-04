package de.l3s.gossen.yarn;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppMaster {
    private static final Logger LOG = LoggerFactory.getLogger(AppMaster.class);

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        LOG.info("Starting app master");
        Configuration conf = new YarnConfiguration();

        LOG.info("Connecting to RM and NM");

        final AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        LOG.info("registerApplicationMaster started");
        rmClient.registerApplicationMaster("", 0, "");
        LOG.info("registerApplicationMaster done");


        int heartBeatInterval = conf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS) / 2;
        heartBeatInterval = Math.min(heartBeatInterval, 10_000);
        final long startTime = System.currentTimeMillis();
        final long waitTime = TimeUnit.MINUTES.toMillis(1);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    long runTime = System.currentTimeMillis() - startTime;
                    float progress = ((float) runTime) / waitTime;
                    float progressPercent = ((int) (progress * 100)) / 100.0f;
                    LOG.info("runtime={}, waittime={}, progress={}", runTime, waitTime, progress);
                    rmClient.allocate(progressPercent);
                } catch (Exception e) {
                    LOG.info("Exception during heartbeat", e);
                }
            }
        }, heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
        LOG.info("Started heartbeat task {} every {} ms", future, heartBeatInterval);

        LOG.info("Going to \"work\" for {} ms", waitTime);
        TimeUnit.MILLISECONDS.sleep(waitTime);
        LOG.info("DONE");

        future.cancel(true);
        executorService.shutdown();
        LOG.info("Shutdown of heartbeat is finished.");
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finished", null);
    }

}
