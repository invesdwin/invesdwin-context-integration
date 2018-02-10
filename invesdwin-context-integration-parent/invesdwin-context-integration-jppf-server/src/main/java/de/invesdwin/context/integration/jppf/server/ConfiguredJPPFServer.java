package de.invesdwin.context.integration.jppf.server;

import java.lang.reflect.Field;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.apache.commons.lang3.BooleanUtils;
import org.jppf.server.JPPFDriver;
import org.jppf.server.nio.acceptor.AcceptorNioServer;
import org.jppf.server.nio.classloader.ClassCache;
import org.jppf.server.nio.classloader.client.ClientClassNioServer;
import org.jppf.server.nio.classloader.node.NodeClassNioServer;
import org.jppf.server.nio.client.ClientNioServer;
import org.jppf.server.nio.nodeserver.NodeNioServer;
import org.jppf.server.node.JPPFNode;

import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;

@ThreadSafe
public final class ConfiguredJPPFServer implements IPreStartupHook, IStartupHook {

    private static final Log LOG = new Log(ConfiguredJPPFServer.class);
    @GuardedBy("ConfiguredJPPFServer.class")
    private static Boolean nodeStartupEnabled;
    private JPPFDriver driver;

    @Inject
    private ConfiguredJPPFNode node;

    public synchronized JPPFDriver getDriver() {
        return driver;
    }

    public synchronized void start() {
        Assertions.checkNull(driver, "already started");
        LOG.info("Starting jppf server at: %s", JPPFServerProperties.getServerBindUri());

        JPPFDriver.main("noLauncher");
        setDriver(JPPFDriver.getInstance());
        assertClassCacheEnabledMatchesConfig();
        Assertions.checkNotNull(driver, "Startup failed!");
        driver.addDriverDiscovery(new ConfiguredPeerDriverDiscovery());
        LOG.info("%s started with UUID: %s", ConfiguredJPPFServer.class.getSimpleName(), driver.getUuid());
    }

    private void assertClassCacheEnabledMatchesConfig() {
        final Field enabledField = Reflections.findField(ClassCache.class, "enabled");
        Reflections.makeAccessible(enabledField);
        final boolean actualServerClassCacheEnabled = (boolean) Reflections.getField(enabledField,
                driver.getInitializer().getClassCache());
        Assertions.assertThat(actualServerClassCacheEnabled).isEqualTo(JPPFServerProperties.SERVER_CLASS_CACHE_ENABLED);
    }

    public synchronized void setDriver(final JPPFDriver instance) {
        Assertions.checkNull(driver, "already started");
        driver = instance;
    }

    @Override
    public synchronized void startup() throws Exception {
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
            final AcceptorNioServer acceptorServer = driver.getAcceptorServer();
            if (acceptorServer != null) {
                acceptorServer.shutdown();
            }
            final ClientClassNioServer clientClassServer = driver.getClientClassServer();
            if (clientClassServer != null) {
                clientClassServer.shutdown();
            }
            final NodeClassNioServer nodeClassServer = driver.getNodeClassServer();
            if (nodeClassServer != null) {
                nodeClassServer.shutdown();
            }
            final NodeNioServer nodeNioServer = driver.getNodeNioServer();
            if (nodeNioServer != null) {
                nodeNioServer.shutdown();
            }
            final ClientNioServer clientNioServer = driver.getClientNioServer();
            if (clientNioServer != null) {
                clientNioServer.shutdown();
            }

            driver.shutdown();
            driver = null;
            if (isNodeStartupEnabled() || JPPFServerProperties.LOCAL_NODE_ENABLED) {
                node.stop();
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
}
