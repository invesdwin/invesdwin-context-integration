package de.invesdwin.context.integration.jppf.server.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.server.ConfiguredJPPFServer;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;

@NotThreadSafe
@Aspect
public class OverrideServerRestartAdminRequestAspect {

    private static final WrappedExecutorService RESTART_EXECUTOR = Executors
            .newFixedThreadPool(OverrideServerRestartAdminRequestAspect.class.getSimpleName(), 1);

    @Around("execution(* org.jppf.server.JPPFDriver.initiateShutdownRestart(..))")

    public Object overrideNodeRestartAdminRequest(final ProceedingJoinPoint pjp) throws Throwable {
        RESTART_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                final ConfiguredJPPFServer server = MergedContext.getInstance().getBean(ConfiguredJPPFServer.class);
                server.stop();
                server.start();
            }
        });
        return null;
    }

}
