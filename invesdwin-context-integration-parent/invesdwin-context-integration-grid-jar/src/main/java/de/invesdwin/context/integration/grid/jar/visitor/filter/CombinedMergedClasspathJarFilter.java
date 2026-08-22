package de.invesdwin.context.integration.grid.jar.visitor.filter;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public class CombinedMergedClasspathJarFilter implements IMergedClasspathJarFilter {

    private final String name;
    private final String[] blacklist;
    private final String[] whitelist;

    public CombinedMergedClasspathJarFilter(final IMergedClasspathJarFilter... filters) {
        this(newCombinedName(filters), filters);
    }

    public CombinedMergedClasspathJarFilter(final String name, final IMergedClasspathJarFilter... filters) {
        this.name = name;
        final List<String> combinedBlacklist = new ArrayList<>();
        final List<String> combinedWhitelist = new ArrayList<>();
        for (final IMergedClasspathJarFilter filter : filters) {
            if (filter.getBlacklist() != null) {
                Collections.addAll(combinedBlacklist, filter.getBlacklist());
            }
            if (filter.getWhitelist() != null) {
                Collections.addAll(combinedWhitelist, filter.getWhitelist());
            }
        }
        this.blacklist = combinedBlacklist.toArray(Strings.EMPTY_ARRAY);
        this.whitelist = combinedWhitelist.toArray(Strings.EMPTY_ARRAY);
    }

    private static String newCombinedName(final IMergedClasspathJarFilter[] filters) {
        final StringBuilder sb = new StringBuilder("COMBINED[");
        for (int i = 0; i < filters.length; i++) {
            if (i > 0) {
                sb.append("+");
            }
            sb.append(filters[i].name());
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String[] getBlacklist() {
        return blacklist;
    }

    @Override
    public String[] getWhitelist() {
        return whitelist;
    }

    public static IMergedClasspathJarFilter ofNullable(final IMergedClasspathJarFilter... filters) {
        final List<IMergedClasspathJarFilter> nonNullFilters = new ArrayList<>(filters.length);
        for (final IMergedClasspathJarFilter filter : filters) {
            if (filter != null) {
                nonNullFilters.add(filter);
            }
        }
        if (nonNullFilters.isEmpty()) {
            return null;
        } else if (nonNullFilters.size() == 1) {
            return nonNullFilters.get(0);
        } else {
            return new CombinedMergedClasspathJarFilter(nonNullFilters.toArray(new IMergedClasspathJarFilter[0]));
        }
    }

}