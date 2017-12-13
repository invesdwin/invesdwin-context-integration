package de.invesdwin.integration.jppf.server;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.server.JPPFDriver;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.util.assertions.Assertions;

@Named
@Immutable
public final class ConfiguredJPPFDriver implements FactoryBean<JPPFDriver>, IStartupHook {

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

    public static synchronized JPPFDriver getInstance() {
        if (instance == null) {
            JPPFDriver.main("noLauncher");
            instance = JPPFDriver.getInstance();
            Assertions.checkNotNull(instance, "Startup failed!");
            instance.addDriverDiscovery(new ConfiguredPeerDriverDiscovery());
        }
        return instance;
    }

    @Override
    public void startup() throws Exception {
        Assertions.checkNotNull(getInstance());
    }
}
