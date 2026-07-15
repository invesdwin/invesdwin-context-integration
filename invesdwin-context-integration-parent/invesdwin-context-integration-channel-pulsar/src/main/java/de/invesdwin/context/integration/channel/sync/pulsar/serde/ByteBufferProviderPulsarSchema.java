package de.invesdwin.context.integration.channel.sync.pulsar.serde;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;

import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public final class ByteBufferProviderPulsarSchema extends AbstractSchema<IByteBufferProvider> {

    private static final ByteBufferProviderPulsarSchema INSTANCE = new ByteBufferProviderPulsarSchema();
    private static final SchemaInfo SCHEMA_INFO = SchemaInfo.builder()
            .name(ByteBufferProviderPulsarSchema.class.getSimpleName())
            .type(SchemaType.BYTES)
            .schema(new byte[0])
            .build();

    private ByteBufferProviderPulsarSchema() {}

    public static Schema<IByteBufferProvider> of() {
        return INSTANCE;
    }

    @Override
    public byte[] encode(final IByteBufferProvider message) {
        try {
            return message.asBuffer().asByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IByteBufferProvider decode(final byte[] bytes) {
        return ByteBuffers.wrap(bytes);
    }

    @Override
    public IByteBufferProvider decode(final java.nio.ByteBuffer data) {
        return ByteBuffers.wrapRelative(data);
    }

    @Override
    public IByteBufferProvider decode(final ByteBuf byteBuf) {
        return new PulsarNettyDelegateByteBuffer(byteBuf);
    }

    //CHECKSTYLE:OFF
    @Override
    public ByteBufferProviderPulsarSchema clone() {
        //CHECKSTYLE:ON
        return INSTANCE;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }

}
