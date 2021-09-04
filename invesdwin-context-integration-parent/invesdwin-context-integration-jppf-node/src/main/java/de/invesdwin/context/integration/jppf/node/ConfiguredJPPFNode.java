package de.invesdwin.context.integration.jppf.node;

import java.net.URI;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.fest.reflect.field.Invoker;
import org.jppf.classloader.AbstractJPPFClassLoader;
import org.jppf.node.NodeRunner;
import org.jppf.server.node.JPPFNode;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.ConfigurationOverridesHandler;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.client.JPPFProcessingThreadsCounter;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.webdav.WebdavFileChannel;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

@ThreadSafe
public final class ConfiguredJPPFNode implements IStartupHook, IShutdownHook {

    private static final WrappedExecutorService NODE_EXECUTOR = Executors
            .newFixedThreadPool(ConfiguredJPPFNode.class.getSimpleName(), 1);
    private static final Log LOG = new Log(ConfiguredJPPFNode.class);
    private boolean startupInvoked = false;
    private boolean startDelayed = false;
    private JPPFNode node;
    @GuardedBy("ConfiguredJPPFNode.class")
    private WebdavFileChannel heartbeatWebdavFileChannel;
    private volatile NodeRunner runner;

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
        final TypedProperties config = JPPFConfiguration.getProperties();
        final TypedProperties defaults = new TypedProperties(config);
        final TypedProperties overrides = new ConfigurationOverridesHandler().load(true);
        if (overrides != null) {
            config.putAll(overrides);
            config.setBoolean("jppf.node.overrides.set", true);
        }
        config.setDefaults(defaults);
        runner = new NodeRunner(config);
        NODE_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                runner.start("noLauncher");
            }
        });
        while (node == null) {
            setNode((JPPFNode) runner.getNode());
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
            try {
                final WebdavFileChannel channel = getHeartbeatWebdavFileChannel(nodeUuid);
                channel.delete();
            } catch (final Throwable e) {
                //ignore
            }

            runner.shutdown();
            try {
                NODE_EXECUTOR.awaitPendingCountZero();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            final Invoker<AbstractJPPFClassLoader> classLoaderField = Reflections.field("classLoader")
                    .ofType(AbstractJPPFClassLoader.class)
                    .in(NodeRunner.class);
            final AbstractJPPFClassLoader classLoader = classLoaderField.get();
            if (classLoader != null) {
                classLoader.close();
                classLoaderField.set(null);
            }
            node = null;
            runner = null;
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
        final JPPFNode node;
        synchronized (this) {
            node = this.node;
        }
        if (node != null) {
            try {
                final String nodeUuid = node.getUuid();
                final int processingThreads = JPPFConfiguration.get(JPPFProperties.PROCESSING_THREADS);
                final FDate heartbeat = new FDate();
                final String content = nodeUuid + JPPFProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + processingThreads + JPPFProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + heartbeat.toString(JPPFProcessingThreadsCounter.WEBDAV_CONTENT_DATEFORMAT);
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
            final URI ftpServerUri = MergedContext.getInstance()
                    .getBean(WebdavServerDestinationProvider.class)
                    .getDestination();
            final WebdavFileChannel channel = new WebdavFileChannel(ftpServerUri,
                    JPPFProcessingThreadsCounter.WEBDAV_DIRECTORY);
            if (!channel.isConnected()) {
                channel.setFilename(JPPFProcessingThreadsCounter.NODE_HEARTBEAT_FILE_PREFIX + nodeUuid + ".heartbeat");
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
