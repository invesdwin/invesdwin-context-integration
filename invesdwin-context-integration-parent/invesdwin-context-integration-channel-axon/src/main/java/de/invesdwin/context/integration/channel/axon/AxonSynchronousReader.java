package de.invesdwin.context.integration.channel.axon;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;

import de.invesdwin.context.integration.channel.axon.channel.AAxonSynchronousChannel;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class AxonSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final Duration DEFAULT_POLL_TIMEOUT = StreamSynchronousEndpointClientReader.DEFAULT_POLL_TIMEOUT;

    protected final AAxonSynchronousChannel channel;
    protected final Duration pollTimeout;
    protected final String topic;

    protected BlockingStream<TrackedEventMessage<?>> stream;
    protected TrackedEventMessage<?> currentMessage;

    public AxonSynchronousReader(final AAxonSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
        this.pollTimeout = newPollTimeout();
    }

    public String getTopic() {
        return topic;
    }

    protected Duration newPollTimeout() {
        return DEFAULT_POLL_TIMEOUT;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        stream = openStream();
    }

    protected BlockingStream<TrackedEventMessage<?>> openStream() {
        return channel.getConfiguration().eventStore().openStream(newTrackingToken());
    }

    protected TrackingToken newTrackingToken() {
        return channel.getConfiguration().eventStore().createTailToken();
    }

    @Override
    public void close() throws IOException {
        if (stream != null) {
            stream.close();
            stream = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentMessage != null) {
            return true;
        }
        try {
            if (stream.hasNextAvailable(pollTimeout.intValue(), pollTimeout.getTimeUnit().timeUnitValue())) {
                final TrackedEventMessage<?> message = stream.nextAvailable();
                if (isMessageValid(message)) {
                    currentMessage = message;
                    return true;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Polling interrupted", e);
        }
        return false;
    }

    protected boolean isMessageValid(final TrackedEventMessage<?> message) {
        if (topic == null) {
            return true;
        }
        return this.topic.equals(message.getMetaData().get("topic"));
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final byte[] payload = (byte[]) currentMessage.getPayload();
        final IByteBuffer msg = ByteBuffers.wrap(payload);
        if (ClosedByteBuffer.isClosed(msg)) {
            close();
            throw FastEOFException.getInstance("Closed by other side");
        }
        return msg;
    }

    @Override
    public void readFinished() throws IOException {
        currentMessage = null;
    }
}