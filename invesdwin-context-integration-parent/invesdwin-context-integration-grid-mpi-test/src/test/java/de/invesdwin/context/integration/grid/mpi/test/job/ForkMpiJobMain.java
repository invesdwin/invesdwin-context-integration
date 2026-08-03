package de.invesdwin.context.integration.grid.mpi.test.job;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.grid.jar.ForkProcessHelper;

@NotThreadSafe
public final class ForkMpiJobMain {

    private ForkMpiJobMain() {}

    public static void main(final String[] args) {
        ForkProcessHelper.fork(MpiJobMain.class, args);
    }

}
