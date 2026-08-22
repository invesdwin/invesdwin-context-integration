package de.invesdwin.context.integration.grid.jar.visitor.filter;

public interface IMergedClasspathJarFilter {

    IMergedClasspathJarFilter[] EMPTY_ARRAY = new IMergedClasspathJarFilter[0];

    String name();

    String[] getBlacklist();

    String[] getWhitelist();

}
