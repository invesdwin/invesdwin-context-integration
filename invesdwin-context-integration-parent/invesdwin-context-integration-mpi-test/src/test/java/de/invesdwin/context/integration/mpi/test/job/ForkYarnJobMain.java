package de.invesdwin.context.integration.mpi.test.job;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ForkYarnJobMain {

    private ForkYarnJobMain() {}

    public static void main(final String[] args) {
        ForkJobHelper.fork(YarnJobMain.class, args);
    }

}
