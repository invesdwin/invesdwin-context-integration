package de.invesdwin.context.integration.grid.jar.fork;

import java.io.File;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class CurrentJavaHomeProvider implements IJavaHomeProvider {

    public static final CurrentJavaHomeProvider INSTANCE = new CurrentJavaHomeProvider();

    private CurrentJavaHomeProvider() {}

    @Override
    public File getJavaHome() {
        //CHECKSTYLE:OFF
        return new File(System.getProperty("java.home"));
        //CHECKSTYLE:ON
    }

}
