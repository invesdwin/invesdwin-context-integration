package de.invesdwin.integration.jppf.node;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.node.NodeRunner;
import org.jppf.server.node.JPPFNode;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.util.assertions.Assertions;

@Named
@Immutable
public final class ConfiguredJPPFNode implements FactoryBean<JPPFNode>, IStartupHook {

    private static boolean createInstance = true;
    private static JPPFNode instance;

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
            NodeRunner.main("noLauncher");
            instance = (JPPFNode) NodeRunner.getNode();
            Assertions.checkNotNull(instance, "Startup failed!");
        }
        return instance;
    }

    public static synchronized void setInstance(final JPPFNode instance) {
        Assertions.checkNull(ConfiguredJPPFNode.instance);
        ConfiguredJPPFNode.instance = instance;
    }

    @Override
    public void startup() throws Exception {
        if (isCreateInstance()) {
            Assertions.checkNotNull(getInstance());
        }
    }
}
