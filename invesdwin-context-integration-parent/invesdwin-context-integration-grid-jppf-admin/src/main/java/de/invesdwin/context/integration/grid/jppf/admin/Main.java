package de.invesdwin.context.integration.grid.jppf.admin;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.JPPFClient;
import org.jppf.ui.console.JPPFAdminConsole;
import org.jppf.ui.utils.JPPFSplash;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.grid.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.grid.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.swing.SunJava2dUIScaleConfigurer;

@Immutable
public final class Main {

    private Main() {}

    public static void main(final String[] args) {
        SunJava2dUIScaleConfigurer.configure();
        final JPPFSplash splash = new JPPFSplash("Connecting ...");
        splash.start();
        MergedContext.autowire(null);
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        final JPPFClient client = JPPFAdminConsole.getTopologyManager().getJPPFClient();
        client.addDriverDiscovery(new ConfiguredClientDriverDiscovery());
        JPPFAdminConsole.main(args);
        splash.stop();
    }

}
