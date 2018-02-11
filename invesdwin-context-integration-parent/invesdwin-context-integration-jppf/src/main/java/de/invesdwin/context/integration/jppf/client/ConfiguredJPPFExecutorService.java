package de.invesdwin.context.integration.jppf.client;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.JPPFClient;
import org.jppf.client.concurrent.JPPFExecutorService;

@ThreadSafe
public class ConfiguredJPPFExecutorService extends JPPFExecutorService {

    public ConfiguredJPPFExecutorService(final JPPFClient client) {
        super(client);
        getConfiguration().getJobConfiguration().getClientSLA().setMaxChannels(-1);
        //this prevents the same task from being executed more than once in parallel, it is better to relaunch the task in a new job instead
        getConfiguration().getJobConfiguration().getSLA().setMaxTaskResubmits(0);
        getConfiguration().getJobConfiguration().getSLA().setApplyMaxResubmitsUponNodeError(true);
    }

}
