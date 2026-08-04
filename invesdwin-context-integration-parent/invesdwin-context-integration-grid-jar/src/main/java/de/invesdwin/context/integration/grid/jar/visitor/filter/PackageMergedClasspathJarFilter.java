package de.invesdwin.context.integration.grid.jar.visitor.filter;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.Arrays;

@Immutable
public class PackageMergedClasspathJarFilter implements IMergedClasspathJarFilter {
    private static final String[] BLACKLIST = new String[] { ".*" };
    private final String[] whitelist;

    public PackageMergedClasspathJarFilter(final String... basePackages) {
        whitelist = new String[basePackages.length];
        for (int i = 0; i < basePackages.length; i++) {
            whitelist[i] = convertPackageNameToPathRegex(basePackages[i]);
        }
    }

    private static String convertPackageNameToPathRegex(final String packageName) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < packageName.length(); i++) {
            final char c = packageName.charAt(i);
            if (c == '.' && (i + 1 < packageName.length() && packageName.charAt(i + 1) != '*')) {
                sb.append('/');
            } else {
                sb.append(c);
            }
        }
        if (sb.length() >= 2 && sb.charAt(sb.length() - 2) != '.' && sb.charAt(sb.length() - 1) != '*') {
            sb.append(".*");
        }
        return sb.toString();
    }

    @Override
    public String name() {
        return "PACKAGE[" + Arrays.hashCode(whitelist) + "]";
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
