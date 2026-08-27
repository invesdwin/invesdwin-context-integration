package de.invesdwin.context.integration.grid.hadoop.test.yarn;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.grid.hadoop.test.HadoopContainer;
import de.invesdwin.context.integration.grid.hadoop.test.yarn.job.YarnJobMain;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.filter.DefaultMergedClasspathJarFilter;
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
        final File jobJarFile = new MergedClasspathJar(DefaultMergedClasspathJarFilter.DEFAULT, YarnJobMain.class)
                .getResource()
                .getFile();
        final File jobScriptFile = new File(ContextProperties.getCacheDirectory(), "yarn_job.sh");

        // 1. Define HDFS channels
        final IFileChannel fileChannel = FileChannelRegistry.newInstance(HADOOP.getHdfsUri());
        final IFileChannel jobJarChannel = fileChannel.withAbsolutePath("/tmp/" + jobJarFile.getName());
        final IFileChannel jobScriptChannel = fileChannel.withAbsolutePath("/tmp/" + jobScriptFile.getName());
        final IFileChannel logDirChannel = fileChannel.withAbsoluteDirectory("/tmp/logs/");

        final File jobScriptTemplate = new ClassPathResource("yarn_job_template.sh", getClass()).getFile();
        String jobScript = Files.readFileToString(jobScriptTemplate, Charset.defaultCharset());
        jobScript = jobScript.replace("{HDFS_JOB_JAR_PATH}", jobJarChannel.getFileUri().toString())
                .replace("{SIZE}", String.valueOf(NUM_CONTAINERS))
                .replace("{HDFS_LOG_DIR}", logDirChannel.getDirectoryUri().toString());
        Files.writeStringToFile(jobScriptFile, jobScript, Charset.defaultCharset());

        // 2. Upload files to HDFS
        jobJarChannel.upload(jobJarFile);
        jobScriptChannel.upload(jobScriptFile);

        runDistributedShellViaProcess(jobJarChannel.getFileUri().toString(), jobScriptChannel.getFileUri().toString());

        // 3. Check logs in HDFS
        if (logDirChannel.exists()) {
            final File localLogsDir = new File(ContextProperties.getCacheDirectory(), "logs");
            de.invesdwin.util.lang.Files.forceMkdir(localLogsDir);
            logDirChannel.withFilename("1_2_LatencyServerTask.log")
                    .download(new File(localLogsDir, "1_2_LatencyServerTask.log"));
            logDirChannel.withFilename("2_2_LatencyClientTask.log")
                    .download(new File(localLogsDir, "2_2_LatencyClientTask.log"));
        }

        final File log_1_2 = new File(ContextProperties.getCacheDirectory(), "logs/1_2_LatencyServerTask.log");
        final File log_2_2 = new File(ContextProperties.getCacheDirectory(), "logs/2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }

    public void runDistributedShellViaProcess(final String hdfsJobJarPathStr, final String hdfsJobScriptPathStr)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();

        final File yarnFile = new File(HadoopContainer.getHadoopHomeFolder(), "bin/yarn");

        // Use the 'yarn' command available in the environment
        command.add(yarnFile.getAbsolutePath());
        command.add("jar");

        final File amJar = new File(HadoopContainer.getHadoopHomeFolder(),
                "share/hadoop/yarn/hadoop-yarn-applications-distributedshell-" + HadoopContainer.HADOOP_VERSION
                        + ".jar");

        // Path to the DistributedShell JAR
        command.add(amJar.getAbsolutePath());

        // Arguments for DistributedShell
        command.add("-jar");
        command.add(amJar.getAbsolutePath());

        command.add("-num_containers");
        command.add(String.valueOf(NUM_CONTAINERS));

        command.add("-shell_command");
        command.add("/home/hduser/hadoop/bin/hadoop fs -get " + hdfsJobScriptPathStr + " . && "
                + "/home/hduser/hadoop/bin/hadoop fs -get " + hdfsJobJarPathStr + " . && " + "sh "
                + new Path(hdfsJobScriptPathStr).getName() + " " + new Path(hdfsJobJarPathStr).getName());

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO(); // Streams YARN logs directly to console

        final Process process = pb.start();
        final int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("YARN job failed with exit code: " + exitCode);
        }
    }
}