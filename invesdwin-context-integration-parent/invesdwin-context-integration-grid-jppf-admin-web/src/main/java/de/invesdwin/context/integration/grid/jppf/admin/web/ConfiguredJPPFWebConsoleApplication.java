package de.invesdwin.context.integration.grid.jppf.admin.web;

import javax.annotation.concurrent.NotThreadSafe;

import org.jppf.admin.web.JPPFWebConsoleApplication;
import org.jppf.admin.web.security.JPPFServletContainerAuthenticatedWebSession;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.grid.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.grid.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConfiguredJPPFWebConsoleApplication extends JPPFWebConsoleApplication {

    @Override
    protected void init() {
        MergedContext.autowire(null);
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        super.init();
        getCspSettings().blocking().disabled();
        getTopologyManager().getJPPFClient().addDriverDiscovery(new ConfiguredClientDriverDiscovery());
    }

    @Override
    protected Class<? extends JPPFServletContainerAuthenticatedWebSession> getContainerManagedWebSessionClass() {
        return ConfiguredJPPFWebSession.class;
    }

}
