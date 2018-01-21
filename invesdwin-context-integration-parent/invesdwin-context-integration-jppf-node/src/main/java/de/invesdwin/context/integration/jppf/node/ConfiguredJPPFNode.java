package de.invesdwin.context.integration.jppf.node;

import java.net.URI;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.jppf.node.NodeRunner;
import org.jppf.server.node.JPPFNode;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.ftp.FtpFileChannel;
import de.invesdwin.context.integration.ftp.FtpServerDestinationProvider;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.client.JPPFProcessingThreadsCounter;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public final class ConfiguredJPPFNode implements IStartupHook, IShutdownHook {

    private static final Log LOG = new Log(ConfiguredJPPFNode.class);
    private boolean startupInvoked = false;
    private boolean startDelayed = false;
    private JPPFNode node;
    @GuardedBy("ConfiguredJPPFNode.class")
    private FtpFileChannel heartbeatFtpFileChannel;

    public synchronized JPPFNode getNode() {
        return node;
    }

    public synchronized void setNode(final JPPFNode node) {
        Assertions.checkNull(this.node, "already started");
        this.node = node;
        if (node != null) {
            uploadHeartbeat();
            LOG.info("%s started with UUID: %s", ConfiguredJPPFNode.class.getSimpleName(), node.getUuid());
        }
    }

    public synchronized void start() {
        Assertions.checkNull(node, "already started");
        Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
        if (!startupInvoked) {
            startDelayed = true;
            return;
        }
        final WrappedExecutorService nodeThread = Executors.newFixedThreadPool(ConfiguredJPPFNode.class.getSimpleName(),
                1);
        JPPFClientProperties.fixSystemProperties();
        nodeThread.execute(new Runnable() {
            @Override
            public void run() {
                NodeRunner.main("noLauncher");
            }
        });
        int triesLeft = 60;
        while (node == null) {
            setNode((JPPFNode) NodeRunner.getNode());
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            triesLeft--;
            if (triesLeft < 0) {
                break;
            }
        }
        Assertions.checkNotNull(node, "Startup failed!");
    }

    @Override
    public synchronized void startup() throws Exception {
        startupInvoked = true;
        if (startDelayed) {
            start();
        }
    }

    public synchronized void stop() {
        if (node != null) {
            final String nodeUuid = node.getUuid();
            final FtpFileChannel channel = getHeartbeatFtpFileChannel(nodeUuid);
            channel.delete();

            node.stopNode();
            try {
                node.stopJmxServer();
            } catch (final Exception e) {
                //ignore
            }
            node = null;
        }
    }

    @Scheduled(initialDelay = 0, fixedDelay = 1 * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND)
    @SkipParallelExecution
    @Retry
    private void scheduledUploadHeartbeat() {
        uploadHeartbeat();
    }

    private void uploadHeartbeat() {
        if (ShutdownHookManager.isShuttingDown()) {
            return;
        }
        final JPPFNode node;
        synchronized (this) {
            node = this.node;
        }
        if (node != null) {
            final String nodeUuid = node.getUuid();
            final int processingThreads = JPPFConfiguration.get(JPPFProperties.PROCESSING_THREADS);
            final FDate heartbeat = new FDate();
            final String content = nodeUuid + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR + processingThreads
                    + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR
                    + heartbeat.toString(JPPFProcessingThreadsCounter.FTP_CONTENT_DATEFORMAT);
            synchronized (this) {
                final FtpFileChannel channel = getHeartbeatFtpFileChannel(nodeUuid);
                channel.upload(content.getBytes());
            }
        }
    }

    @Retry
    private FtpFileChannel getHeartbeatFtpFileChannel(final String nodeUuid) {
        if (heartbeatFtpFileChannel == null || !heartbeatFtpFileChannel.getFilename().contains(nodeUuid)
                || !heartbeatFtpFileChannel.isConnected()) {
            if (heartbeatFtpFileChannel != null) {
                heartbeatFtpFileChannel.close();
                heartbeatFtpFileChannel = null;
            }
            final URI ftpServerUri = MergedContext.getInstance()
                    .getBean(FtpServerDestinationProvider.class)
                    .getDestination();
            final FtpFileChannel channel = new FtpFileChannel(ftpServerUri, JPPFProcessingThreadsCounter.FTP_DIRECTORY);
            if (!channel.isConnected()) {
                channel.setFilename("node_" + nodeUuid + ".heartbeat");
                channel.connect();
            }
            heartbeatFtpFileChannel = channel;
        }
        return heartbeatFtpFileChannel;
    }

    @Override
    public void shutdown() throws Exception {
        stop();
    }

}
