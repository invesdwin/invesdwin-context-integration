package de.invesdwin.context.integration.grid.jar.fork;

import java.io.File;

public interface IJavaHomeProvider {

    File getJavaHome();

    default File getJavaCommand() {
        return new File(getJavaHome(), "bin/java");
    }

}
