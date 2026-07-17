package de.invesdwin.context.integration.channel.sync.axon;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.eventstore.EventStore;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AxonSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    protected final AxonSynchronousChannel channel;
    protected final String topic;
    protected Map<String, String> metaData;
    protected EventStore eventStore;

    public AxonSynchronousWriter(final AxonSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        this.metaData = newMetaData();
        this.eventStore = channel.getConfiguration().eventStore();
    }

    protected Map<String, String> newMetaData() {
        if (topic == null) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap("topic", topic);
        }
    }

    @Override
    public void close() throws IOException {
        if (metaData != null) {
            write(ClosedByteBuffer.INSTANCE);
            channel.close();
            metaData = null;
            eventStore = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return channel.getConfiguration() != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final byte[] bytes = message.asBuffer().asByteArrayCopy();
        final EventMessage<byte[]> eventMessage = newEventMessage(bytes);
        eventStore.publish(eventMessage);
    }

    @SuppressWarnings("null")
    protected EventMessage<byte[]> newEventMessage(final byte[] bytes) {
        return new GenericEventMessage<byte[]>(bytes, metaData);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }
}