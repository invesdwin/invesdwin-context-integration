package de.invesdwin.context.integration.grid.jar.visitor;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PackageMergedClasspathJarFilter implements IMergedClasspathJarFilter {
    private static final String[] BLACKLIST = new String[] { ".*" };
    private final String[] whitelist;

    public PackageMergedClasspathJarFilter(final String... basePackages) {
        whitelist = new String[basePackages.length];
        for (int i = 0; i < basePackages.length; i++) {
            whitelist[i] = basePackages[i] + ".*";
        }
    }

    @Override
    public String name() {
        return PackageMergedClasspathJarFilter.class.getSimpleName();
    }

    @Override
    public String[] getBlacklist() {
        return BLACKLIST;
    }

    @Override
    public String[] getWhitelist() {
        return whitelist;
    }

}
