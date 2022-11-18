package de.invesdwin.context.integration.jppf.server;

import java.lang.management.ManagementFactory;
import java.net.URI;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Inject;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang3.BooleanUtils;
import org.jppf.management.spi.JPPFDriverMBeanProvider;
import org.jppf.server.JPPFDriver;
import org.jppf.server.node.JPPFNode;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;
import org.jppf.utils.hooks.Hook;
import org.jppf.utils.hooks.HookFactory;
import org.jppf.utils.hooks.HookInstance;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.client.JPPFProcessingThreadsCounter;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.webdav.WebdavFileChannel;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

@ThreadSafe
public final class ConfiguredJPPFServer implements IPreStartupHook, IStartupHook, IShutdownHook {

    private static final Log LOG = new Log(ConfiguredJPPFServer.class);
    @GuardedBy("ConfiguredJPPFServer.class")
    private static Boolean nodeStartupEnabled;
    private JPPFDriver driver;
    @GuardedBy("ConfiguredJPPFServer.class")
    private WebdavFileChannel heartbeatWebdavFileChannel;

    @Inject
    private ConfiguredJPPFNode node;

    public synchronized JPPFDriver getDriver() {
        return driver;
    }

    public synchronized void start() {
        Assertions.checkNull(driver, "already started");
        LOG.info("Starting jppf server at: %s", JPPFServerProperties.getServerBindUri());

        driver = new JPPFDriver(JPPFConfiguration.getProperties());
        try {
            driver.start();
        } catch (final Exception e) {
            driver = null;
            throw new RuntimeException(e);
        }
        driver.addDriverDiscovery(new ConfiguredPeerDriverDiscovery());
        uploadHeartbeat();
        LOG.info("%s started with UUID: %s", ConfiguredJPPFServer.class.getSimpleName(), driver.getUuid());
        if (MergedContext.isBootstrapFinished()) {
            startup();
        }
    }

    @Override
    public synchronized void startup() {
        if (driver != null) {
            if (JPPFServerProperties.LOCAL_NODE_ENABLED) {
                final JPPFNode localNode = Reflections.field("localNode").ofType(JPPFNode.class).in(driver).get();
                node.setNode(localNode);
                Assertions.assertThat(node.getNode()).isSameAs(localNode);
            } else {
                if (isNodeStartupEnabled()) {
                    node.start();
                    Assertions.checkNotNull(node.getNode());
                }
            }
        }
    }

    public synchronized void stop() {
        if (driver != null) {
            driver.shutdown();
            if (isNodeStartupEnabled() || JPPFServerProperties.LOCAL_NODE_ENABLED) {
                node.stop();
            }
            unregisterMBeans(JPPFDriverMBeanProvider.class);
            driver = null;
        }
    }

    private <S> void unregisterMBeans(final Class<S> clazz) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Hook<S> hook = HookFactory.registerSPIMultipleHook(clazz, null, loader);
        for (final HookInstance<S> hookInstance : hook.getInstances()) {
            try {
                final String mbeanName = (String) hookInstance.invoke("getMBeanName");
                server.unregisterMBean(new ObjectName(mbeanName));
            } catch (final Exception e) {
                //ignore
            }
        }
    }

    public static synchronized void setNodeStartupEnabled(final Boolean nodeStartupEnabled) {
        ConfiguredJPPFServer.nodeStartupEnabled = nodeStartupEnabled;
    }

    public static synchronized boolean isNodeStartupEnabled() {
        return BooleanUtils.isTrue(nodeStartupEnabled);
    }

    @Override
    public void preStartup() throws Exception {
        synchronized (ConfiguredJPPFServer.class) {
            if (nodeStartupEnabled == null) {
                //refresh the value
                nodeStartupEnabled = JPPFNodeContextLocation.isActivated();
                if (nodeStartupEnabled || JPPFServerProperties.LOCAL_NODE_ENABLED) {
                    JPPFNodeContextLocation.deactivate();
                }
            }
        }
    }

    @Override
    public void shutdown() throws Exception {
        stop();
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
        final JPPFDriver driver;
        synchronized (this) {
            driver = this.driver;
        }
        if (driver != null) {
            try {
                final String driverUuid = driver.getUuid();
                final int processingThreads = JPPFConfiguration.get(JPPFProperties.PROCESSING_THREADS);
                final FDate heartbeat = new FDate();
                final String content = driverUuid + JPPFProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + processingThreads + JPPFProcessingThreadsCounter.WEBDAV_CONTENT_SEPARATOR
                        + heartbeat.toString(JPPFProcessingThreadsCounter.WEBDAV_CONTENT_DATEFORMAT);
                synchronized (this) {
                    final WebdavFileChannel channel = getHeartbeatWebdavFileChannel(driverUuid);
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

    private WebdavFileChannel getHeartbeatWebdavFileChannel(final String driverUuid) {
        final boolean differentDriverUuid = heartbeatWebdavFileChannel != null
                && heartbeatWebdavFileChannel.getFilename() != null
                && !heartbeatWebdavFileChannel.getFilename().contains(driverUuid);
        if (heartbeatWebdavFileChannel == null || differentDriverUuid || !heartbeatWebdavFileChannel.isConnected()) {
            if (heartbeatWebdavFileChannel != null) {
                if (differentDriverUuid) {
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
                channel.setFilename(
                        JPPFProcessingThreadsCounter.DRIVER_HEARTBEAT_FILE_PREFIX + driverUuid + ".heartbeat");
                channel.connect();
            }
            heartbeatWebdavFileChannel = channel;
        }
        return heartbeatWebdavFileChannel;
    }

}
