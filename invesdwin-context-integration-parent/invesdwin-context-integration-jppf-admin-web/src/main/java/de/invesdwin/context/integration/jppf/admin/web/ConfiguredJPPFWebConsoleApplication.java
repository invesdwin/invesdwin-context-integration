package de.invesdwin.context.integration.jppf.admin.web;

import javax.annotation.concurrent.NotThreadSafe;

import org.jppf.admin.web.JPPFWebConsoleApplication;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConfiguredJPPFWebConsoleApplication extends JPPFWebConsoleApplication {

    @Override
    protected void init() {
        MergedContext.autowire(null);
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        super.init();
        getTopologyManager().getJPPFClient().addDriverDiscovery(new ConfiguredClientDriverDiscovery());
    }

}
