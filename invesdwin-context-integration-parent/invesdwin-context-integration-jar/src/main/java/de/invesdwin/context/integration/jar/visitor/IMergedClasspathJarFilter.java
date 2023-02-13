package de.invesdwin.context.integration.jar.visitor;

public interface IMergedClasspathJarFilter {

    String[] getBlacklist();

    String[] getWhitelist();

}
