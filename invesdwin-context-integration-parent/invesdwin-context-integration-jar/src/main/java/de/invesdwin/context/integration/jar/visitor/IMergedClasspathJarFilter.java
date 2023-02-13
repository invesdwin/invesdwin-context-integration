package de.invesdwin.context.integration.jar.visitor;

public interface IMergedClasspathJarFilter {
    String name();

    String[] getBlacklist();

    String[] getWhitelist();

}
