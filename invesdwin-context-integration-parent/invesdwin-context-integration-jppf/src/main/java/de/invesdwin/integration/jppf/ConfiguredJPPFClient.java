package de.invesdwin.integration.jppf;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.concurrent.Executors;

@Named
@NotThreadSafe
public final class ConfiguredJPPFClient extends JPPFClient implements FactoryBean<ConfiguredJPPFClient> {

    public static final ConfiguredJPPFClient INSTANCE;

    static {
        final TypedProperties props = JPPFConfiguration.getProperties();
        //use custom discovery only
        props.set(JPPFProperties.DISCOVERY_ENABLED, false);
        //might be a security risk
        props.set(JPPFProperties.CLASSLOADER_FILE_LOOKUP, false);
        //allow local execution if enabled
        props.set(JPPFProperties.LOCAL_EXECUTION_ENABLED, JPPFClientProperties.LOCAL_EXECUTION_ENABLED);
        props.set(JPPFProperties.LOCAL_EXECUTION_THREADS, Executors.getCpuThreadPoolCount());
        INSTANCE = new ConfiguredJPPFClient();
    }

    private ConfiguredJPPFClient() {
        super();
    }

    @Override
    public ConfiguredJPPFClient getObject() throws Exception {
        return INSTANCE;
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

}
