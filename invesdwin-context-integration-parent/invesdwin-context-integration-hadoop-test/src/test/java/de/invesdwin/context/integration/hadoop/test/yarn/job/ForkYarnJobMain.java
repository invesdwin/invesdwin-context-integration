package de.invesdwin.context.integration.hadoop.test.yarn.job;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.jar.ForkJobHelper;

@NotThreadSafe
public final class ForkYarnJobMain {

    private ForkYarnJobMain() {}

    public static void main(final String[] args) {
        ForkJobHelper.fork(YarnJobMain.class, args);
    }

}
