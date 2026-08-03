package de.invesdwin.context.integration.grid.jar.visitor.filter;

public interface IMergedClasspathJarFilter {
    String name();

    String[] getBlacklist();

    String[] getWhitelist();

}
