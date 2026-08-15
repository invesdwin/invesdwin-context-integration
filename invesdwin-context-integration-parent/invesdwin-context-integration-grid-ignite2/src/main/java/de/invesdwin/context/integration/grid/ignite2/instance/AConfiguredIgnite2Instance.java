package de.invesdwin.context.integration.grid.ignite2.instance;

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class AConfiguredIgnite2Instance implements IStartupHook, IShutdownHook, FactoryBean<Ignite> {

    protected final Log log = new Log(getClass());

    private boolean startupInvoked = false;
    private boolean startDelayed = false;

    private volatile Ignite instance;

    @GuardedBy("this")
    private IFileChannel heartbeatWebdavFileChannel;
    private Ignite2InstanceProcessingThreadsCounter processingThreadsCounter;

    protected abstract IgniteConfiguration createConfiguration();

    public synchronized Ignite getInstance() {
        return instance;
    }

    @Override
    public Ignite getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return Ignite.class;
    }

    public synchronized Ignite2InstanceProcessingThreadsCounter getProcessingThreadsCounter() {
        return processingThreadsCounter;
    }

    private synchronized void setInstance(final Ignite instance) {
        Assertions.checkNull(this.instance, "already started");
        this.instance = instance;
        if (instance != null) {
            processingThreadsCounter = new Ignite2InstanceProcessingThreadsCounter(instance);
            uploadHeartbeat();
            log.info("%s started with name: %s", getClass().getSimpleName(), instance.name());
        }
    }

    public synchronized void start() {
        Assertions.checkNull(instance, "already started");

        if (!startupInvoked) {
            startDelayed = true;
            return;
        }

        final IgniteConfiguration configuration = createConfiguration();

        configuration.setWorkDirectory(
                new File(ContextProperties.getCacheDirectory(), getClass().getSimpleName()).getAbsolutePath());

        // Enable Peer Class Loading across cluster nodes
        configuration.setPeerClassLoadingEnabled(true);

        // Ensure topology events required by Ignite2ProcessingThreadsCounter are always enabled
        configuration.setIncludeEventTypes(EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT,
                EventType.EVT_NODE_FAILED);

        final Ignite startedNode = Ignition.start(configuration);
        setInstance(startedNode);

        waitForWarmup();
    }

    private void waitForWarmup() {
        try {
            processingThreadsCounter.waitForMinimumCounts(1, Duration.ONE_MINUTE);
        } catch (final TimeoutException e) {
            //ignore
        }
        processingThreadsCounter.logWarmupFinished();
    }

    @Override
    public synchronized void startup() throws Exception {
        startupInvoked = true;
        if (startDelayed) {
            start();
        }
    }

    public synchronized void stop() {
        if (instance != null) {
            final String igniteName = instance.name();
            final String nodeUuid = instance.cluster().localNode().id().toString();

            try {
                final IFileChannel channel = getHeartbeatWebdavFileChannel(nodeUuid);
                channel.delete();
            } catch (final Throwable e) {
                //ignore
            }

            try {
                instance.close();
            } catch (final Exception e) {
                log.error("Error shutting down Ignite node", e);
            }

            instance = null;
            processingThreadsCounter = null;
            log.info("%s stopped: %s", getClass().getSimpleName(), igniteName);
        }
    }

    @Scheduled(initialDelay = 0, fixedDelay = 1 * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND)
    @SkipParallelExecution
    private void scheduledUploadHeartbeat() {
        uploadHeartbeat();
    }

    @Retry
    private void uploadHeartbeat() {
        if (ShutdownHookManager.isShuttingDown()) {
            return;
        }
        final Ignite localIgnite;
        synchronized (this) {
            localIgnite = this.instance;
        }
        if (localIgnite != null) {
            try {
                final String hostname = IntegrationProperties.HOSTNAME;
                final ClusterNode localNode = localIgnite.cluster().localNode();
                final String nodeUuid = localNode.id().toString();
                final int processingThreads = localNode.metrics().getTotalCpus();
                final FDate heartbeat = FDate.now();

                final String content = hostname + Ignite2InstanceProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + nodeUuid + Ignite2InstanceProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + processingThreads + Ignite2InstanceProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + heartbeat.toString(Ignite2InstanceProcessingThreadsCounter.WEBDAV_CONTENT_DATEFORMAT);

                synchronized (this) {
                    final IFileChannel channel = getHeartbeatWebdavFileChannel(nodeUuid);
                    try {
                        channel.upload(content.getBytes());
                    } catch (final Throwable t) {
                        channel.close();
                        throw t;
                    }
                }
            } catch (final Throwable t) {
                throw new RetryLaterRuntimeException(t);
            }
        }
    }

    private IFileChannel getHeartbeatWebdavFileChannel(final String nodeUuid) {
        final boolean differentNodeUuid = heartbeatWebdavFileChannel != null
                && heartbeatWebdavFileChannel.getFilename() != null
                && !heartbeatWebdavFileChannel.getFilename().contains(nodeUuid);

        if (heartbeatWebdavFileChannel == null || differentNodeUuid || !heartbeatWebdavFileChannel.isConnected()) {
            if (heartbeatWebdavFileChannel != null) {
                if (differentNodeUuid) {
                    try {
                        if (!heartbeatWebdavFileChannel.isConnected()) {
                            heartbeatWebdavFileChannel.connect();
                        }
                        heartbeatWebdavFileChannel.delete();
                    } catch (final Throwable t) {
                        //ignore
                    }
                }
                heartbeatWebdavFileChannel.close();
                heartbeatWebdavFileChannel = null;
            }
            final URI webdavServerUri = MergedContext.getInstance()
                    .getBean(WebdavServerDestinationProvider.class)
                    .getDestination();
            final IFileChannel channel = FileChannelRegistry.newInstance(webdavServerUri)
                    .setSubDirectory(Ignite2InstanceProcessingThreadsCounter.WEBDAV_DIRECTORY);
            if (!channel.isConnected()) {
                final String prefix = instance.cluster().localNode().isClient()
                        ? Ignite2InstanceProcessingThreadsCounter.DRIVER_HEARTBEAT_FILE_PREFIX
                        : Ignite2InstanceProcessingThreadsCounter.NODE_HEARTBEAT_FILE_PREFIX;
                channel.setFilename(prefix + nodeUuid + ".heartbeat");
                channel.connect();
            }
            heartbeatWebdavFileChannel = channel;
        }
        return heartbeatWebdavFileChannel;
    }

    @Override
    public void shutdown() throws Exception {
        stop();
    }
}