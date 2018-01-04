package de.invesdwin.context.integration.jppf.client;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.JPPFClient;
import org.jppf.client.concurrent.JPPFExecutorService;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;

@ThreadSafe
public class ConfiguredJPPFExecutorService extends JPPFExecutorService {

    public ConfiguredJPPFExecutorService(final JPPFClient client) {
        super(client);
        getConfiguration().getJobConfiguration().getClientSLA().setMaxChannels(JPPFClientProperties.IO_CHANNELS);
    }

}
