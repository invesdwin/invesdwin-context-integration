package de.invesdwin.integration.jppf.server;

import javax.annotation.concurrent.Immutable;

import org.jppf.server.JPPFDriver;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;

@Immutable
public final class ConfiguredJPPFDriver implements FactoryBean<JPPFDriver> {

    public static final JPPFDriver INSTANCE;

    static {
        final TypedProperties props = JPPFConfiguration.getProperties();
        props.set(JPPFProperties.DISCOVERY_ENABLED, false);
        //reduce overhead by having a peer to peer network that consists of servers that have local nodes
        props.set(JPPFProperties.LOCAL_NODE_ENABLED, true);
        props.set(JPPFProperties.PROCESSING_THREADS, Executors.getCpuThreadPoolCount());
        //might be a security risk
        props.set(JPPFProperties.CLASSLOADER_FILE_LOOKUP, false);
        JPPFDriver.main(String.valueOf(JPPFServerProperties.SERVER_PORT));
        INSTANCE = JPPFDriver.getInstance();
        Assertions.checkNotNull(INSTANCE, "Startup failed!");
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
