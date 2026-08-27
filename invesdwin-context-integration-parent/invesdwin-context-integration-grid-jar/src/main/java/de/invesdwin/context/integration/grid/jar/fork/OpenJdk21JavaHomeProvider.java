package de.invesdwin.context.integration.grid.jar.fork;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.lock.FileChannelLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.date.millis.FDateMillis;

@ThreadSafe
public class OpenJdk21JavaHomeProvider implements IJavaHomeProvider {

    public static final OpenJdk21JavaHomeProvider INSTANCE = new OpenJdk21JavaHomeProvider();

    private static final String OPENJDK_MAJOR_VERSION = "21";
    private static final String OPENJDK_VERSION = OPENJDK_MAJOR_VERSION + ".0.11_10";
    private static final String OPENJDK_DOWNLOAD_URL;
    private static final File OPENJDK_FOLDER;
    private static final File OPENJDK_EXTRACTED_FOLDER;

    static {
        PlatformInitializerProperties.setAllowed(false);
        OPENJDK_FOLDER = new File(ContextProperties.getHomeDirectory(), "openjdk" + OPENJDK_VERSION);
        final String folderVersion = OPENJDK_VERSION.replace("_", "+");
        OPENJDK_EXTRACTED_FOLDER = new File(OPENJDK_FOLDER, "jdk-" + folderVersion);

        OPENJDK_DOWNLOAD_URL = "https://github.com/adoptium/temurin" + OPENJDK_MAJOR_VERSION
                + "-binaries/releases/download/jdk-" + URIs.encode(folderVersion) + "/OpenJDK" + OPENJDK_MAJOR_VERSION
                + "U-jdk_x64_linux_hotspot_" + OPENJDK_VERSION + ".tar.gz";
    }

    @Override
    public File getJavaHome() {
        for (final String potentialJavaHome : new String[] {
                "/usr/lib/jvm/java-" + OPENJDK_MAJOR_VERSION + "-openjdk-amd64",
                "/usr/lib/jvm/java-" + OPENJDK_MAJOR_VERSION + "-openjdk" }) {
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
            long started = FDateMillis.nowMillis();
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
                    "Finished downloading [" + javaTarGz + "] after " + (FDateMillis.nowMillis() - started) + " ms");
            //CHECKSTYLE:ON

            started = FDateMillis.nowMillis();
            //CHECKSTYLE:OFF
            System.out.println("Started extracting [" + javaTarGz + "]");
            //CHECKSTYLE:ON
            final Archiver archiver = ArchiverFactory.createArchiver(javaTarGz);
            archiver.extract(javaTarGz, OPENJDK_FOLDER);
            Assertions.assertThat(OPENJDK_EXTRACTED_FOLDER).exists();
            Files.deleteQuietly(javaTarGz);
            //CHECKSTYLE:OFF
            System.out.println(
                    "Finished extracting [" + javaTarGz + "] after " + (FDateMillis.nowMillis() - started) + " ms");
            //CHECKSTYLE:ON
            return OPENJDK_EXTRACTED_FOLDER;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
