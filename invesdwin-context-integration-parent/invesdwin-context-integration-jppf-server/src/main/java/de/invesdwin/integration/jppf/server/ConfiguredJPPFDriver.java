package de.invesdwin.integration.jppf.server;

import javax.annotation.concurrent.Immutable;

import org.jppf.server.JPPFDriver;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.assertions.Assertions;

@Immutable
public final class ConfiguredJPPFDriver implements FactoryBean<JPPFDriver> {

    public static final JPPFDriver INSTANCE;

    static {
        JPPFDriver.main(String.valueOf(JPPFServerProperties.SERVER_PORT));
        INSTANCE = JPPFDriver.getInstance();
        Assertions.checkNotNull(INSTANCE, "Startup failed!");
        INSTANCE.addDriverDiscovery(new ConfiguredPeerDriverDiscovery());
    }

    private ConfiguredJPPFDriver() {}

    @Override
    public JPPFDriver getObject() throws Exception {
        return INSTANCE;
    }

    @Override
    public Class<?> getObjectType() {
        return JPPFDriver.class;
    }

}
