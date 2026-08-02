package de.invesdwin.context.integration.grid.mpi.test.job;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.grid.jar.ForkJobHelper;

@NotThreadSafe
public final class ForkMpiJobMain {

    private ForkMpiJobMain() {}

    public static void main(final String[] args) {
        ForkJobHelper.fork(MpiJobMain.class, args);
    }

}
