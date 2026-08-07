package de.invesdwin.context.integration.grid.ignite3.instance;

import java.net.URI;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite3.AIgnite3ProcessingThreadsCounter;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.webdav.WebdavFileChannel;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class AConfiguredIgnite3Instance<S> implements IStartupHook, IShutdownHook, FactoryBean<Ignite> {

    protected final Log log = new Log(getClass());
    private final WrappedExecutorService executor = Executors.newFixedThreadPool(getClass().getSimpleName(), 1);

    private boolean startupInvoked = false;
    private boolean startDelayed = false;

    private volatile S server;

    @GuardedBy("this")
    private WebdavFileChannel heartbeatWebdavFileChannel;
    private Ignite3InstanceProcessingThreadsCounter processingThreadsCounter;

    // Delegated to subclass to handle the actual server/client node startup[cite: 40]
    protected abstract S startIgniteServer();

    public synchronized S getServer() {
        return server;
    }

    public synchronized Ignite getInstance() {
        return getApi(server);
    }

    protected abstract Ignite getApi(S server);

    @Override
    public Ignite getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return Ignite.class;
    }

    public synchronized Ignite3InstanceProcessingThreadsCounter getProcessingThreadsCounter() {
        return processingThreadsCounter;
    }

    private synchronized void setInstance(final S server) {
        Assertions.checkNull(this.server, "already started");
        this.server = server;
        if (server != null) {
            final Ignite instance = getInstance();
            processingThreadsCounter = new Ignite3InstanceProcessingThreadsCounter(instance);
            uploadHeartbeat();
            log.info("%s started with name: %s", getClass().getSimpleName(), instance.name());
        }
    }

    public synchronized void start() {
        Assertions.checkNull(server, "already started");

        if (!startupInvoked) {
            startDelayed = true;
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                // Ignite 3 server configuration and startup is entirely delegated[cite: 40]
                final S server = startIgniteServer();
                setInstance(server);
            }
        });

        while (server == null) {
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Ignite to start", e);
            }
        }

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
        if (server != null) {
            final String igniteName = getInstance().name();

            try {
                final WebdavFileChannel channel = getHeartbeatWebdavFileChannel(igniteName);
                channel.delete();
            } catch (final Throwable e) {
                //ignore
            }

            try {
                shutdown(server);
            } catch (final Exception e) {
                log.error("Error shutting down Ignite node", e);
            }

            try {
                executor.awaitPendingCountZero();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            server = null;
            processingThreadsCounter = null;
            log.info("%s stopped: %s", getClass().getSimpleName(), igniteName);
        }
    }

    protected abstract void shutdown(S server);

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
            localIgnite = getInstance();
        }
        if (localIgnite != null) {
            try {
                final String hostname = IntegrationProperties.HOSTNAME;
                // Ignite 3 node identification acts directly on the Ignite instance instead of cluster local nodes[cite: 40]
                final String nodeUuid = localIgnite.name();
                final int processingThreads = Runtime.getRuntime().availableProcessors();
                final FDate heartbeat = FDate.now();

                final String content = hostname + AIgnite3ProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR + nodeUuid
                        + AIgnite3ProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR + processingThreads
                        + AIgnite3ProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + heartbeat.toString(AIgnite3ProcessingThreadsCounter.WEBDAV_CONTENT_DATEFORMAT);

                synchronized (this) {
                    final WebdavFileChannel channel = getHeartbeatWebdavFileChannel(nodeUuid);
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

    private WebdavFileChannel getHeartbeatWebdavFileChannel(final String nodeUuid) {
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
            final WebdavFileChannel channel = new WebdavFileChannel(webdavServerUri)
                    .setSubDirectory(AIgnite3ProcessingThreadsCounter.WEBDAV_DIRECTORY);
            if (!channel.isConnected()) {
                final String prefix = AIgnite3ProcessingThreadsCounter.NODE_HEARTBEAT_FILE_PREFIX;
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