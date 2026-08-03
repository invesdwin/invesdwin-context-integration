package de.invesdwin.context.integration.grid.hadoop.test.yarn.job;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.grid.jar.ForkProcessHelper;

@NotThreadSafe
public final class ForkYarnJobMain {

    private ForkYarnJobMain() {}

    public static void main(final String[] args) {
        ForkProcessHelper.fork(YarnJobMain.class, args);
    }

}
