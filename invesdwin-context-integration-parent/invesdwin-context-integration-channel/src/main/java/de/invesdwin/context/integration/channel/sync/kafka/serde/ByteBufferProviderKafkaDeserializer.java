package de.invesdwin.context.integration.channel.sync.kafka.serde;

import javax.annotation.concurrent.Immutable;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class ByteBufferProviderKafkaDeserializer implements Deserializer<IByteBufferProvider> {

    @Override
    public IByteBufferProvider deserialize(final String topic, final byte[] data) {
        return ByteBuffers.wrap(data);
    }

    @Override
    public IByteBufferProvider deserialize(final String topic, final Headers headers, final java.nio.ByteBuffer data) {
        return ByteBuffers.wrap(data);
    }

}
