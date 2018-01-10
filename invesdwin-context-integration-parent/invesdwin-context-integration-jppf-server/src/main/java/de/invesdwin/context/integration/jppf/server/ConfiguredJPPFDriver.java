package de.invesdwin.context.integration.jppf.server;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.server.JPPFDriver;
import org.jppf.server.node.JPPFNode;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;

@Named
@Immutable
public final class ConfiguredJPPFDriver implements FactoryBean<JPPFDriver>, IStartupHook, IPreStartupHook {

    private static final Log LOG = new Log(ConfiguredJPPFDriver.class);
    private static boolean createInstance = true;
    private static JPPFDriver instance;

    private ConfiguredJPPFDriver() {}

    @Override
    public JPPFDriver getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return JPPFDriver.class;
    }

    public static synchronized boolean isCreateInstance() {
        return createInstance;
    }

    public static synchronized void setCreateInstance(final boolean createInstance) {
        ConfiguredJPPFDriver.createInstance = createInstance;
    }

    public static synchronized JPPFDriver getInstance() {
        if (instance == null && createInstance) {
            LOG.info("Starting jppf server at: %s", JPPFServerProperties.getServerBindUri());
            JPPFDriver.main("noLauncher");
            instance = JPPFDriver.getInstance();
            Assertions.checkNotNull(instance, "Startup failed!");
            instance.addDriverDiscovery(new ConfiguredPeerDriverDiscovery());
            if (JPPFServerProperties.LOCAL_NODE_ENABLED) {
                final JPPFNode localNode = Reflections.field("localNode").ofType(JPPFNode.class).in(instance).get();
                ConfiguredJPPFNode.setInstance(localNode);
                Assertions.assertThat(ConfiguredJPPFNode.getInstance()).isSameAs(localNode);
            }
        }
        return instance;
    }

    public static void setInstance(final JPPFDriver instance) {
        ConfiguredJPPFDriver.instance = instance;
    }

    @Override
    public void startup() throws Exception {
        if (isCreateInstance()) {
            Assertions.checkNotNull(getInstance());
        }

    }

    @Override
    public void preStartup() throws Exception {
        if (JPPFServerProperties.LOCAL_NODE_ENABLED) {
            ConfiguredJPPFNode.setCreateInstance(false);
        }
    }
}
