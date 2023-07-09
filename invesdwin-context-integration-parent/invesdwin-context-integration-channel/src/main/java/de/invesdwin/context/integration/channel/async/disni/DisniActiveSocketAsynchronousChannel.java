package de.invesdwin.context.integration.channel.async.disni;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;

@NotThreadSafe
public class DisniActiveSocketAsynchronousChannel implements IAsynchronousChannel {

    private AsynchronousDisniActiveSynchronousChannel channel;

    public DisniActiveSocketAsynchronousChannel(final AsynchronousDisniActiveSynchronousChannel channel) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean isClosed() {
        return channel == null || channel.isClosed();
    }

}
