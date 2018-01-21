package de.invesdwin.context.integration.jppf.server;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;

import de.invesdwin.context.beans.init.AMain;

@NotThreadSafe
public final class Main extends AMain {

    private Main(final String[] args) {
        super(args);
    }

    @Override

    protected void startApplication(final CmdLineParser parser) throws Exception {
        waitForShutdown();
    }

    public static void main(final String[] args) {
        JPPFServerContextLocation.activate();
        new Main(args);
    }
}
