package de.invesdwin.context.integration.channel.sync.aeron;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;

@NotThreadSafe
public abstract class AAeronSynchronousChannel implements ISynchronousChannel {

    private static MediaDriver embeddedMediaDriver;

    protected final AeronMediaDriverMode mode;
    protected final String channel;
    protected final int streamId;
    protected Aeron aeron;

    public AAeronSynchronousChannel(final AeronMediaDriverMode mode, final String channel, final int streamId) {
        this.mode = mode;
        this.channel = channel;
        this.streamId = streamId;
    }

    public static synchronized MediaDriver getEmbeddedMediadriver() {
        if (embeddedMediaDriver == null) {
            embeddedMediaDriver = newDefaultEmbeddedMediaDriver();
        }
        return embeddedMediaDriver;
    }

    public static synchronized void setEmbeddedMediaDriver(final MediaDriver mediaDriver) {
        AAeronSynchronousChannel.embeddedMediaDriver = mediaDriver;
    }

    public static MediaDriver newDefaultEmbeddedMediaDriver() {
        return MediaDriver.launchEmbedded(
                new Context().dirDeleteOnShutdown(true).dirDeleteOnStart(true).threadingMode(ThreadingMode.DEDICATED));
    }

    @Override
    public void open() throws IOException {
        this.aeron = Aeron.connect(newContext());
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

    @Override
    public void close() throws IOException {
        if (aeron != null) {
            aeron.close();
            aeron = null;
        }
    }

}
