package de.invesdwin.context.integration.axon.channel;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;

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

    private final AxonSynchronousChannel channel;
    private final Duration pollTimeout;
    private final String topic;

    private BlockingStream<TrackedEventMessage<?>> eventStream;
    private TrackedEventMessage<?> currentMessage;

    public AxonSynchronousReader(final AxonSynchronousChannel channel, final String topic) {
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
        final TrackingToken token = channel.getConfiguration().eventStore().createTailToken();
        eventStream = channel.getConfiguration().eventStore().openStream(token);
    }

    @Override
    public void close() throws IOException {
        if (eventStream != null) {
            eventStream.close();
            eventStream = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentMessage != null) {
            return true;
        }
        try {
            if (eventStream.hasNextAvailable(pollTimeout.intValue(), pollTimeout.getTimeUnit().timeUnitValue())) {
                final TrackedEventMessage<?> message = eventStream.nextAvailable();

                if (this.topic.equals(message.getMetaData().get("topic"))) {
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

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final AxonChannelMessage payload = (AxonChannelMessage) currentMessage.getPayload();
        final IByteBuffer msg = ByteBuffers.wrap(payload.getBytes());
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