package de.invesdwin.context.integration.mpi.test;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextDirectoriesStub;
import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public abstract class AMpiTest extends ATest {

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(ContextDirectoriesStub.class);
        Files.deleteNative(ContextProperties.getCacheDirectory());
        Files.forceMkdir(ContextProperties.getCacheDirectory());
    }

}
