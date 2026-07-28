package de.invesdwin.context.integration.mpi.test.job;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public class SparkJobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-m", aliases = "--master", usage = "Defines the Spark master URL")
    protected String master;

    public SparkJobMain() {
        super(Strings.EMPTY_ARRAY, BOOTSTRAP);
    }

    public SparkJobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        runSparkJob(logDir, size, master);
    }

    private static void runSparkJob(final String logDir, final int size, final String master) {
        // Run locally with threads equal to container count
        final SparkConf conf = new SparkConf().setAppName(SparkJobMain.class.getSimpleName());
        if (master != null) {
            conf.setMaster(master);
        }

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            final List<Integer> ranks = Arrays.asList(0, 1);

            sc.parallelize(ranks, size).mapPartitions(iterator -> {
                if (iterator.hasNext()) {
                    final int rank = iterator.next();

                    final String[] args = { "--size", String.valueOf(size), "--rank", String.valueOf(rank), "--logDir",
                            logDir };

                    // Execute logic
                    YarnJobMain.main(args);
                }
                return Collections.singletonList(true).iterator();
            }).collect();
        }
    }

    public static void main(final String[] args) {
        new SparkJobMain(args).run();
    }
}