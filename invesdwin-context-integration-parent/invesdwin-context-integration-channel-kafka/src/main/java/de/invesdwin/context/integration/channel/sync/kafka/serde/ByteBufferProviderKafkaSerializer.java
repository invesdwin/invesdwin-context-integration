package de.invesdwin.context.integration.channel.sync.kafka.serde;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.kafka.common.serialization.Serializer;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class ByteBufferProviderKafkaSerializer implements Serializer<IByteBufferProvider> {

    @Override
    public byte[] serialize(final String topic, final IByteBufferProvider data) {
        try {
            return data.asBuffer().asByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
