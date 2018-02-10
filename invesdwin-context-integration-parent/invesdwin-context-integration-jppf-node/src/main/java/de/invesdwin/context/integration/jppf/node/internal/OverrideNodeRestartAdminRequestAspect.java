package de.invesdwin.context.integration.jppf.node.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;

@NotThreadSafe
@Aspect
public class OverrideNodeRestartAdminRequestAspect {

    private static final WrappedExecutorService RESTART_EXECUTOR = Executors
            .newFixedThreadPool(OverrideNodeRestartAdminRequestAspect.class.getSimpleName(), 1);

    @Around("execution(* org.jppf.server.node.JPPFNode.shutdown(..))")

    public Object overrideNodeRestartAdminRequest(final ProceedingJoinPoint pjp) throws Throwable {
        RESTART_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                final ConfiguredJPPFNode node = MergedContext.getInstance().getBean(ConfiguredJPPFNode.class);
                node.stop();
                node.start();
            }
        });
        return null;
    }

}
