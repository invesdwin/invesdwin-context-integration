package de.invesdwin.context.integration.grid.jar.visitor.filter;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum DistributionMergedClasspathJarFilter implements IMergedClasspathJarFilter {
    DISTRIBUTION {
        @Override
        public String[] getBlacklist() {
            return DEFAULT_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return DEFAULT_WHITELIST;
        }
    };

    private static final String[] DEFAULT_BLACKLIST = { "META-INF/env/.*\\.properties" };

    private static final String[] DEFAULT_WHITELIST = { "META-INF/env/distribution\\.properties" };
}
