package de.invesdwin.context.integration.grid.jar.visitor;

public interface IMergedClasspathJarFilter {
    String name();

    String[] getBlacklist();

    String[] getWhitelist();

}
