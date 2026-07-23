package de.invesdwin.context.integration.mpi.test.job;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ForkMpiJobMain {

    private ForkMpiJobMain() {}

    public static void main(final String[] args) {
        ForkJobHelper.fork(MpiJobMain.class, args);
    }

}
