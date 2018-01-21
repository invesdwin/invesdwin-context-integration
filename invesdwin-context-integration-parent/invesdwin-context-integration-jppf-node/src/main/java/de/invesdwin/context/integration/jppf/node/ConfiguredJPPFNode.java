package de.invesdwin.context.integration.jppf.node;

import java.net.URI;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.node.NodeRunner;
import org.jppf.server.node.JPPFNode;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.beans.factory.FactoryBean;
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

@Named
@Immutable
public final class ConfiguredJPPFNode implements FactoryBean<JPPFNode>, IStartupHook, IShutdownHook {

    private static final Log LOG = new Log(ConfiguredJPPFNode.class);
    private static boolean createInstance = !JPPFClientProperties.LOCAL_EXECUTION_ENABLED;
    private static JPPFNode instance;
    @GuardedBy("ConfiguredJPPFNode.class")
    private static FtpFileChannel heartbeatFtpFileChannel;

    private ConfiguredJPPFNode() {}

    @Override
    public JPPFNode getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return JPPFNode.class;
    }

    public static synchronized boolean isCreateInstance() {
        return createInstance;
    }

    public static synchronized void setCreateInstance(final boolean createInstance) {
        ConfiguredJPPFNode.createInstance = createInstance;
    }

    public static synchronized JPPFNode getInstance() {
        if (instance == null && createInstance) {
            Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
            final WrappedExecutorService nodeThread = Executors
                    .newFixedThreadPool(ConfiguredJPPFNode.class.getSimpleName(), 1);
            nodeThread.execute(new Runnable() {
                @Override
                public void run() {
                    NodeRunner.main("noLauncher");
                }
            });
            int triesLeft = 60;
            while (instance == null) {
                setInstance((JPPFNode) NodeRunner.getNode());
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
            Assertions.checkNotNull(instance, "Startup failed!");
        }
        return instance;
    }

    public static synchronized void setInstance(final JPPFNode instance) {
        Assertions.checkNull(ConfiguredJPPFNode.instance);
        ConfiguredJPPFNode.instance = instance;
        if (instance != null) {
            uploadHeartbeat();
            LOG.info("%s started with UUID: %s", ConfiguredJPPFNode.class.getSimpleName(), instance.getUuid());
        }
    }

    @Override
    public void startup() throws Exception {
        if (isCreateInstance()) {
            Assertions.checkNotNull(getInstance());
        }
    }

    @Scheduled(initialDelay = 0, fixedDelay = 1 * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND)
    @SkipParallelExecution
    @Retry
    private void scheduledUploadHeartbeat() {
        uploadHeartbeat();
    }

    private static void uploadHeartbeat() {
        if (ShutdownHookManager.isShuttingDown()) {
            return;
        }
        final JPPFNode node;
        synchronized (ConfiguredJPPFNode.class) {
            node = instance;
        }
        if (node != null) {
            final String nodeUuid = node.getUuid();
            final int processingThreads = JPPFConfiguration.get(JPPFProperties.PROCESSING_THREADS);
            final FDate heartbeat = new FDate();
            final String content = nodeUuid + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR + processingThreads
                    + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR
                    + heartbeat.toString(JPPFProcessingThreadsCounter.FTP_CONTENT_DATEFORMAT);
            synchronized (ConfiguredJPPFNode.class) {
                final FtpFileChannel channel = getHeartbeatFtpFileChannel(nodeUuid);
                channel.upload(content.getBytes());
            }
        }
    }

    @Retry
    private static FtpFileChannel getHeartbeatFtpFileChannel(final String nodeUuid) {
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
        final JPPFNode node;
        synchronized (ConfiguredJPPFNode.class) {
            node = instance;
        }
        if (node != null) {
            final String nodeUuid = node.getUuid();
            synchronized (ConfiguredJPPFNode.class) {
                final FtpFileChannel channel = getHeartbeatFtpFileChannel(nodeUuid);
                channel.delete();
            }
        }
    }
}
