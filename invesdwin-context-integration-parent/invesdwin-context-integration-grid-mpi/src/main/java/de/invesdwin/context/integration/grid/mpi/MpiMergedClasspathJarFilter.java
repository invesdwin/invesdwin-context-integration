package de.invesdwin.context.integration.grid.mpi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.grid.jar.visitor.DefaultMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.jar.visitor.IMergedClasspathJarFilter;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public enum MpiMergedClasspathJarFilter implements IMergedClasspathJarFilter {
    MPI {
        @Override
        public String[] getBlacklist() {
            return MPI_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return Strings.EMPTY_ARRAY;
        }
    };

    private static final String[] MPI_BLACKLIST = Arrays.concat(DefaultMergedClasspathJarFilter.DEFAULT.getBlacklist(),
            new String[] { "mpi/.*", "mpjbuf/.*", "mpjdev/.*", "xdev/.*" });

}
