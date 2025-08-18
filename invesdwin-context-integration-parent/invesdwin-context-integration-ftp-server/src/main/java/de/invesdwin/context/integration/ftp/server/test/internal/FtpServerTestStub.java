package de.invesdwin.context.integration.ftp.server.test.internal;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ftpserver.FtpServer;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.ContextDirectoriesStub;
import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.ftp.server.FtpServerContextLocation;
import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.context.integration.ftp.server.test.FtpServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import jakarta.inject.Named;

@Named
@NotThreadSafe
public class FtpServerTestStub extends StubSupport {

    private static volatile FtpServer lastServer;

    static {
        ShutdownHookManager.register(new IShutdownHook() {
            @Override
            public void shutdown() throws Exception {
                maybeStopLastServer();
            }
        });
        ContextDirectoriesStub.addProtectedDirectory(FtpServerProperties.WORKING_DIRECTORY);
    }

    @Override
    public void setUpContextLocations(final ATest test, final List<PositionedResource> locations) throws Exception {
        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
        final FtpServerTest annotation = Reflections.getAnnotation(test, FtpServerTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(FtpServerContextLocation.CONTEXT_LOCATION);
            } else {
                locations.remove(FtpServerContextLocation.CONTEXT_LOCATION);
            }
        }
    }

    @Override
    public void setUpContext(final ATest test, final TestContext ctx) throws Exception {
        //clean up for next test
        Files.deleteQuietly(FtpServerProperties.WORKING_DIRECTORY);
        Files.forceMkdir(FtpServerProperties.WORKING_DIRECTORY);
    }

    @Override
    public void setUpOnce(final ATest test, final TestContext ctx) throws Exception {
        try {
            FtpServerTestStub.lastServer = MergedContext.getInstance().getBean(FtpServer.class);
        } catch (final NoSuchBeanDefinitionException e) { //SUPPRESS CHECKSTYLE empty block
            //ignore
        }
    }

    @Override
    public void tearDownOnce(final ATest test, final TestContext ctx) throws Exception {
        if (!ctx.isFinished()) {
            return;
        }
        maybeStopLastServer();
    }

    private static void maybeStopLastServer() throws Exception {
        if (lastServer != null) {
            lastServer.stop();
            lastServer = null;
        }
    }

}
