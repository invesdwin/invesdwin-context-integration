package de.invesdwin.context.integration.channel.aeron;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;

@NotThreadSafe
public abstract class AAeronSynchronousChannel implements ISynchronousChannel {

    private static MediaDriver mediaDriver = newDefaultMediaDriver();

    protected final String channel;
    protected final int streamId;
    protected Aeron aeron;

    public AAeronSynchronousChannel(final String channel, final int streamId) {
        this.channel = channel;
        this.streamId = streamId;
    }

    public static synchronized MediaDriver getMediadriver() {
        if (mediaDriver == null) {
            mediaDriver = newDefaultMediaDriver();
        }
        return mediaDriver;
    }

    public static synchronized void setMediaDriver(final MediaDriver mediaDriver) {
        AAeronSynchronousChannel.mediaDriver = mediaDriver;
    }

    public static MediaDriver newDefaultMediaDriver() {
        return MediaDriver.launchEmbedded(
                new Context().dirDeleteOnShutdown(true).dirDeleteOnStart(true).threadingMode(ThreadingMode.SHARED));
    }

    @Override
    public void open() throws IOException {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(getMediadriver().aeronDirectoryName()));
    }

    @Override
    public void close() throws IOException {
        if (aeron != null) {
            aeron.close();
            aeron = null;
        }
    }

}
