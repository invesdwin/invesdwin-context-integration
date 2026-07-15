package de.invesdwin.context.integration.channel.sync.axon;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AxonSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final AxonSynchronousChannel channel;
    private final String topic;
    private Map<String, String> metaData;

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
        this.metaData = Collections.singletonMap("topic", topic);
    }

    @Override
    public void close() throws IOException {
        if (metaData != null) {
            write(ClosedByteBuffer.INSTANCE);
            channel.close();
            metaData = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return channel.getConfiguration() != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final byte[] bytes = message.asBuffer().asByteArrayCopy();
        final EventMessage<Object> eventMessage = GenericEventMessage.asEventMessage(new AxonChannelMessage(bytes))
                .withMetaData(metaData);
        channel.getConfiguration().eventStore().publish(eventMessage);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }
}