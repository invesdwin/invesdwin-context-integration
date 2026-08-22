package de.invesdwin.context.integration.grid.jar.visitor.filter;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.string.Strings;

@Immutable
public enum DefaultMergedClasspathJarFilter implements IMergedClasspathJarFilter {
    DEFAULT {
        @Override
        public String[] getBlacklist() {
            return DEFAULT_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return Strings.EMPTY_ARRAY;
        }
    };

    //<filters>
    //    <filter>
    //        <artifact>*:*</artifact>
    //        <excludes>
    //            <exclude>META-INF/*.SF</exclude>
    //            <exclude>META-INF/*.DSA</exclude>
    //            <exclude>META-INF/*.RSA</exclude>
    //        </excludes>
    //    </filter>
    //</filters>
    private static final String[] DEFAULT_BLACKLIST = { "META-INF/.*\\.SF", "META-INF/.*\\.DSA", "META-INF/.*\\.RSA",
            "META-INF/MANIFEST\\.MF", "META-INF/INDEX\\.LIST" };

}
