package de.invesdwin.context.integration.channel.stream.client.channel;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.stream.client.IStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.stream.client.IStreamSynchronousEndpointClientSubscription;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class StreamSynchronousEndpointClientReader implements ISynchronousReader<IByteBufferProvider> {

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ONE_MILLISECOND;
    private final StreamSynchronousEndpointClientChannel channel;
    private final boolean closeMessageEnabled;
    private final Duration pollTimeout;
    private IStreamSynchronousEndpointClient client;
    private IByteBufferProvider polledMessage;
    private IByteBuffer polledMessageBuffer;

    public StreamSynchronousEndpointClientReader(final StreamSynchronousEndpointClientChannel channel) {
        this.channel = channel;
        this.closeMessageEnabled = channel.isCloseMessageEnabled();
        this.pollTimeout = newPollTimeout();
    }

    /**
     * Here one can also define Duration.ZERO to do non-blocking polling.
     */
    protected Duration newPollTimeout() {
        return DEFAULT_POLL_TIMEOUT;
    }

    protected FDate newFromTimestamp() {
        return null;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        polledMessageBuffer = ByteBuffers.allocateExpandable();
        this.client = channel.getClient();
        client.subscribe(channel.getServiceId(), channel.newSubscribeTopicUri(newFromTimestamp()),
                new IStreamSynchronousEndpointClientSubscription() {
                    @Override
                    public void onPush(final int serviceId, final String topic, final IByteBufferProvider message) {
                        //since this might be the active reader message tied to the the async client, we have to make a safe copy here
                        try {
                            final int length = message.getBuffer(polledMessageBuffer);
                            polledMessage = polledMessageBuffer.sliceTo(length);
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            polledMessageBuffer = null;
            polledMessage = null;
            client.unsubscribe(channel.getServiceId(), channel.newUnsubscribeTopicUri());
            client = null;
            channel.close();
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledMessage != null) {
            return true;
        }
        if (client == null) {
            throw FastEOFException.getInstance("already closed");
        }
        return client.poll(pollTimeout);
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (closeMessageEnabled) {
            final IByteBuffer messageBuffer = polledMessage.asBuffer();
            if (messageBuffer.getByte(0) == ClosedByteBuffer.CLOSED_BYTE
                    && (messageBuffer.capacity() == 1 || messageBuffer.getByte(1) == 0)) {
                close();
                throw FastEOFException.getInstance("closed by other side");
            }
            return messageBuffer;
        } else {
            return polledMessage;
        }
    }

    @Override
    public void readFinished() {
        polledMessage = null;
    }

}
