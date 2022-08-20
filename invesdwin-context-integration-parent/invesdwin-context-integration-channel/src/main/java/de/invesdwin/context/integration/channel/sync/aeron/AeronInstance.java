package de.invesdwin.context.integration.channel.sync.aeron;

import java.io.File;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;

/**
 * One instance per media driver is sufficient.
 */
@ThreadSafe
public class AeronInstance {

    public static final String AERON_IPC_CHANNEL = "aeron:ipc";
    private static MediaDriver embeddedMediaDriver;
    private final AeronMediaDriverMode mode;

    @GuardedBy("this")
    private int usageCount;
    @GuardedBy("this")
    private Aeron aeron;

    public AeronInstance(final AeronMediaDriverMode mode) {
        this.mode = mode;
    }

    public static synchronized MediaDriver getEmbeddedMediadriver() {
        if (embeddedMediaDriver == null) {
            embeddedMediaDriver = newDefaultEmbeddedMediaDriver();
        }
        return embeddedMediaDriver;
    }

    public static synchronized void setEmbeddedMediaDriver(final MediaDriver mediaDriver) {
        embeddedMediaDriver = mediaDriver;
    }

    public static MediaDriver newDefaultEmbeddedMediaDriver() {
        return newEmbeddedMediaDriver(new File(ContextProperties.TEMP_DIRECTORY,
                AAeronSynchronousChannel.class.getSimpleName() + "_" + "_default").getAbsolutePath());
    }

    public static MediaDriver newEmbeddedMediaDriver(final String name) {
        return MediaDriver.launchEmbedded(new Context().aeronDirectoryName(name)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED));
    }

    /**
     * Override this method to specify a different directory for native media driver (aeronmd application).
     */
    protected io.aeron.Aeron.Context newContext() {
        if (mode == AeronMediaDriverMode.EMBEDDED) {
            return new Aeron.Context().aeronDirectoryName(getEmbeddedMediadriver().aeronDirectoryName());
        } else {
            return new Aeron.Context();
        }
    }

    public synchronized boolean isClosed() {
        return aeron == null;
    }

    public synchronized Aeron open() {
        usageCount++;
        if (aeron == null) {
            aeron = Aeron.connect(newContext());
        }
        return aeron;
    }

    public synchronized void close(final Aeron aeron) {
        Assertions.checkSame(this.aeron, aeron);
        usageCount--;
        if (usageCount == 0) {
            this.aeron.close();
            this.aeron = null;
        }
    }

}
