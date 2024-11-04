package de.invesdwin.context.integration.mpi.test.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;

@NotThreadSafe
public final class MpiJobYarnMain {

    private static final String OPENJDK_VERSION = "17.0.6_10";
    private static final String OPENJDK_DOWNLOAD_URL;
    private static final File OPENJDK_FOLDER;
    private static final File OPENJDK_EXTRACTED_FOLDER;

    static {
        PlatformInitializerProperties.setAllowed(false);
        OPENJDK_FOLDER = new File(ContextProperties.getHomeDirectory(), "openjdk" + OPENJDK_VERSION);
        final String folderVersion = OPENJDK_VERSION.replace("_", "+");
        OPENJDK_EXTRACTED_FOLDER = new File(OPENJDK_FOLDER, "jdk-" + folderVersion);
        OPENJDK_DOWNLOAD_URL = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-"
                + URIs.encode(folderVersion) + "/OpenJDK17U-jdk_x64_linux_hotspot_" + OPENJDK_VERSION + ".tar.gz";
    }

    private MpiJobYarnMain() {}

    public static void main(final String[] args) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        StringBuilder classpath = new StringBuilder();
        for (final URL url : DynamicInstrumentationReflections.getURLs(classLoader)) {
            classpath.append(url.toString());
            classpath.append(File.pathSeparator);
        }
        classpath = Strings.removeEnd(classpath, ":");

        try {
            //upgrade to java 17 from java 11 (hadoop does not support java 17 yet)
            final File javaHome = maybeDownloadAndExtractJava17();
            final List<String> commands = new ArrayList<>();
            commands.add(javaHome.getAbsolutePath() + "/bin/java");
            commands.add("-classpath");
            commands.add(classpath.toString());
            commands.add(MpiJobMain.class.getName());
            for (int i = 0; i < args.length; i++) {
                commands.add(args[i]);
            }
            new ProcessExecutor().command(commands)
                    .destroyOnExit()
                    .exitValueNormal()
                    .redirectOutput(Slf4jStream.of(MpiJobYarnMain.class).asInfo())
                    .redirectError(Slf4jStream.of(MpiJobYarnMain.class).asWarn())
                    .environment(System.getenv())
                    .stopper(new ProcessStopper() {
                        @Override
                        public void stop(final Process process) {
                            process.destroy();
                        }
                    })
                    .execute();
        } catch (final Throwable e) {
            throw Err.process(e);
        }
        //kill any outstanding threads
        System.exit(0);
    }

    private static File maybeDownloadAndExtractJava17() {
        for (final String potentialJavaHome : new String[] { "/usr/lib/jvm/java-17-openjdk-amd64",
                "/usr/lib/jvm/java-17-openjdk" }) {
            final File f = new File(potentialJavaHome);
            if (f.exists()) {
                return f;
            }
        }

        final File javaTarGz = new File(OPENJDK_FOLDER, "openjdk-" + OPENJDK_VERSION + ".tar.gz");
        if (OPENJDK_EXTRACTED_FOLDER.exists() && !javaTarGz.exists()) {
            return OPENJDK_EXTRACTED_FOLDER;
        }

        try (FileChannelLock fileChannelLock = new FileChannelLock(new File(javaTarGz.getAbsolutePath() + ".lock"))) {
            fileChannelLock.lock();
            if (OPENJDK_EXTRACTED_FOLDER.exists() && !javaTarGz.exists()) {
                return OPENJDK_EXTRACTED_FOLDER;
            }
            long started = System.currentTimeMillis();
            //CHECKSTYLE:OFF
            System.out.println("Started downloading [" + javaTarGz + "]");
            //CHECKSTYLE:ON
            final File javaTarGzPart = new File(javaTarGz.getAbsolutePath() + ".part");
            Files.deleteQuietly(javaTarGzPart);
            Files.deleteQuietly(javaTarGz);
            try (InputStream in = URIs.connect(OPENJDK_DOWNLOAD_URL).downloadInputStream()) {
                try (FileOutputStream out = new FileOutputStream(javaTarGzPart)) {
                    IOUtils.copy(in, out);
                }
            }
            Files.moveFileQuietly(javaTarGzPart, javaTarGz);
            //CHECKSTYLE:OFF
            System.out.println(
                    "Finished downloading [" + javaTarGz + "] after " + (System.currentTimeMillis() - started) + " ms");
            //CHECKSTYLE:ON

            started = System.currentTimeMillis();
            //CHECKSTYLE:OFF
            System.out.println("Started extracting [" + javaTarGz + "]");
            //CHECKSTYLE:ON
            final Archiver archiver = ArchiverFactory.createArchiver(javaTarGz);
            archiver.extract(javaTarGz, OPENJDK_FOLDER);
            Assertions.assertThat(OPENJDK_EXTRACTED_FOLDER).exists();
            Files.deleteQuietly(javaTarGz);
            //CHECKSTYLE:OFF
            System.out.println(
                    "Finished extracting [" + javaTarGz + "] after " + (System.currentTimeMillis() - started) + " ms");
            //CHECKSTYLE:ON
            return OPENJDK_EXTRACTED_FOLDER;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
