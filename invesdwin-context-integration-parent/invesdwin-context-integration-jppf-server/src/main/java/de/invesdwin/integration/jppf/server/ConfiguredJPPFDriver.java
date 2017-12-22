package de.invesdwin.integration.jppf.server;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.server.JPPFDriver;
import org.jppf.server.node.JPPFNode;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;

@Named
@Immutable
public final class ConfiguredJPPFDriver implements FactoryBean<JPPFDriver>, IStartupHook, IPreStartupHook {

    private static boolean createInstance;
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
        Assertions.checkNotNull(getInstance());
    }

    @Override
    public void preStartup() throws Exception {
        if (JPPFServerProperties.LOCAL_NODE_ENABLED) {
            ConfiguredJPPFNode.setCreateInstance(false);
        }
    }
}
