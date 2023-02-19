package de.invesdwin.context.integration.jar.visitor;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public enum MergedClasspathJarFilter implements IMergedClasspathJarFilter {
    DEFAULT {
        @Override
        public String[] getBlacklist() {
            return DEFAULT_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return Strings.EMPTY_ARRAY;
        }
    },
    HADOOP2 {
        @Override
        public String[] getBlacklist() {
            return HADOOP2_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return HADOOP_WHITELIST;
        }
    },
    HADOOP3 {
        @Override
        public String[] getBlacklist() {
            return HADOOP3_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return HADOOP_WHITELIST;
        }
    },
    MPI {
        @Override
        public String[] getBlacklist() {
            return MPI_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return Strings.EMPTY_ARRAY;
        }
    },
    MPI_YARN2 {
        @Override
        public String[] getBlacklist() {
            return MPI_YARN2_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return MPI_YARN_WHITELIST;
        }
    },
    MPI_YARN3 {
        @Override
        public String[] getBlacklist() {
            return MPI_YARN3_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return MPI_YARN_WHITELIST;
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
            "META-INF/MANIFEST.MF", "META-INF/INDEX.LIST" };

    // exclude packages that hadoop has itself in classpath to prevent clashes
    private static final String[] HADOOP3_BLACKLIST = Arrays.concat(DEFAULT_BLACKLIST,
            new String[] { "org/apache/hadoop/.*", "javax/inject/.*", "org/codehaus/jackson/.*", "org/mockito/.*",
                    "org/apache/commons/collections/.*", "org/apache/commons/compress/.*", "org/apache/commons/math/.*",
                    "org/aopalliance/.*", "org/apache/commons/lang/.*", "org/apache/avro/.*",
                    "com/thoughtworks/paranamer/.*", "org/junit/.*", "org/objenesis/.*", "org/hamcrest/.*",
                    "org/apache/commons/beanutils/.*", "org/xerial/snappy/.*", "javax/annotation/.*",
                    "org/apache/commons/configuration/.*", "org/slf4j/.*", "org/apache/log4j/.*",
                    "org/apache/commons/io/.*", "org/apache/commons/codec/.*", "junit/.*",
                    "org/apache/commons/logging/.*", "javax/el/.*", "org/apache/commons/cli/.*",
                    "org/codehaus/jettison/.*", "META-INF/services/javax\\.cache\\.spi\\.CachingProvider" });

    // hadoop 3 uses shaded protobuf and guava
    private static final String[] HADOOP2_BLACKLIST = Arrays.concat(HADOOP3_BLACKLIST,
            new String[] { "com/google/protobuf/.*", "com/google/common/.*", });

    private static final String[] HADOOP_WHITELIST = { "org/slf4j/ext/.*" };

    private static final String[] MPI_BLACKLIST = Arrays.concat(DEFAULT_BLACKLIST,
            new String[] { "mpi/.*", "mpjbuf/.*", "mpjdev/.*", "xdev/.*" });

    private static final String[] MPI_YARN2_BLACKLIST = Arrays.concat(HADOOP2_BLACKLIST, MPI_BLACKLIST);
    private static final String[] MPI_YARN3_BLACKLIST = Arrays.concat(HADOOP3_BLACKLIST, MPI_BLACKLIST);
    private static final String[] MPI_YARN_WHITELIST = HADOOP_WHITELIST;

}
