package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.hadoop.docker.HadoopContainer;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.YarnJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
@Testcontainers
public class YarnDistributedShellTest extends ATest {

    private static final int NUM_CONTAINERS = 2;
    @Container
    private static final HadoopContainer HADOOP = new HadoopContainer();

    @Test
    public void test() throws Exception {

        final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.MPI, YarnJobMain.class).getResource()
                .getFile();
        final File jobScriptFile = new File(ContextProperties.getCacheDirectory(), "yarn_job.sh");

        // 1. Define HDFS paths
        final Path hdfsJobJarPath = new Path("/tmp/" + jobJarFile.getName());
        final Path hdfsJobScriptPath = new Path("/tmp/" + jobScriptFile.getName());

        final File jobScriptTemplate = new File("mpj/job/yarn_job_template.sh");
        String jobScript = Files.readFileToString(jobScriptTemplate, Charset.defaultCharset());
        jobScript = jobScript.replace("{HDFS_JOB_JAR_PATH}", hdfsJobJarPath.toString())
                .replace("{SIZE}", String.valueOf(NUM_CONTAINERS));
        Files.writeStringToFile(jobScriptFile, jobScript, Charset.defaultCharset());

        final YarnConfiguration conf = HADOOP.newYarnConfiguration();
        final FileSystem fs = FileSystem.get(conf);

        // 2. Upload files to HDFS (overwrite = true)
        fs.copyFromLocalFile(false, true, new Path(jobJarFile.getAbsolutePath()), hdfsJobJarPath);
        fs.copyFromLocalFile(false, true, new Path(jobScriptFile.getAbsolutePath()), hdfsJobScriptPath);

        runDistributedShellViaProcess(hdfsJobJarPath, hdfsJobScriptPath);

        // 3. Check logs in HDFS
        final Path hdfsLogDir = new Path("/tmp/logs/");
        if (fs.exists(hdfsLogDir)) {
            fs.copyToLocalFile(hdfsLogDir, new Path(ContextProperties.getCacheDirectory().getAbsolutePath()));
        }
        final File log_1_2 = new File(ContextProperties.getCacheDirectory(), "logs/1_2_LatencyServerTask.log");
        final File log_2_2 = new File(ContextProperties.getCacheDirectory(), "logs/2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains(" 100% ");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains(" 100% ");
    }

    public void runDistributedShellViaProcess(final Path hdfsJobJarPath, final Path hdfsJobScriptPath)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();

        final File yarnFile = new File(HADOOP.getHadoopFolder(), "bin/yarn");

        // Use the 'yarn' command available in the environment
        command.add(yarnFile.getAbsolutePath());
        command.add("jar");

        final File amJar = new File(HADOOP.getHadoopFolder(),
                "share/hadoop/yarn/hadoop-yarn-applications-distributedshell-" + HADOOP.getHadoopVersion() + ".jar");

        // Path to the DistributedShell JAR (you can find this in your Hadoop install)
        command.add(amJar.getAbsolutePath());

        // Arguments for DistributedShell
        command.add("-jar"); // The DistributedShell app master JAR
        command.add(amJar.getAbsolutePath());

        command.add("-num_containers");
        command.add(String.valueOf(NUM_CONTAINERS));

        command.add("-shell_command");
        command.add("/home/hduser/hadoop/bin/hadoop fs -get " + hdfsJobScriptPath.toString() + " . && "
                + "/home/hduser/hadoop/bin/hadoop fs -get " + hdfsJobJarPath.toString() + " . && " + "sh "
                + hdfsJobScriptPath.getName() + " " + hdfsJobJarPath.getName());

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO(); // This streams the YARN logs directly to your JUnit console!

        final Process process = pb.start();
        final int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("YARN job failed with exit code: " + exitCode);
        }
    }
}