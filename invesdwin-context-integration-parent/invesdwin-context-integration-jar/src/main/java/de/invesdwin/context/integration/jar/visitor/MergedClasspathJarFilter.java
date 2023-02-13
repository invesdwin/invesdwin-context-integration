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
    HADOOP {
        @Override
        public String[] getBlacklist() {
            return HADOOP_BLACKLIST;
        }

        @Override
        public String[] getWhitelist() {
            return HADOOP_WHITELIST;
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

    private static final String[] HADOOP_BLACKLIST = Arrays.concat(DEFAULT_BLACKLIST, new String[] {
            // exclude packages that hadoop has itself in classpath to prevent clashes
            "org/apache/hadoop/.*", "javax/inject/.*", "org/codehaus/jackson/.*", "org/mockito/.*",
            "org/apache/commons/collections/.*", "com/google/common/.*", "org/apache/commons/compress/.*",
            "com/google/protobuf/.*", "org/apache/commons/math/.*", "org/aopalliance/.*", "org/apache/commons/lang/.*",
            "org/apache/avro/.*", "com/thoughtworks/paranamer/.*", "org/junit/.*", "org/objenesis/.*",
            "org/hamcrest/.*", "org/apache/commons/beanutils/.*", "org/xerial/snappy/.*", "javax/annotation/.*",
            "org/apache/commons/configuration/.*", "org/slf4j/.*", "org/apache/log4j/.*", "org/apache/commons/io/.*",
            "org/apache/commons/codec/.*", "junit/.*", "org/apache/commons/logging/.*", "javax/el/.*",
            "org/apache/commons/cli/.*", "org/codehaus/jettison/.*" });

    private static final String[] HADOOP_WHITELIST = { "org/slf4j/ext/.*" };

}
