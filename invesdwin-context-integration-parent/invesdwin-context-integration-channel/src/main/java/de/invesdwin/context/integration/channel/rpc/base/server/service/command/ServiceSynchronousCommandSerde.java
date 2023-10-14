package de.invesdwin.context.integration.channel.rpc.base.server.service.command;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class ServiceSynchronousCommandSerde<M> implements ISerde<IServiceSynchronousCommand<M>> {

    public static final int SERVICE_INDEX = 0;
    public static final int SERVICE_SIZE = Integer.BYTES;

    public static final int METHOD_INDEX = SERVICE_INDEX + SERVICE_SIZE;
    public static final int METHOD_SIZE = Integer.BYTES;

    public static final int SEQUENCE_INDEX = METHOD_INDEX + METHOD_SIZE;
    public static final int SEQUENCE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private final ISerde<M> messageSerde;
    private final Integer messageFixedLength;
    private final int fixedLength;

    public ServiceSynchronousCommandSerde(final ISerde<M> messageSerde, final Integer messageFixedLength) {
        this.messageSerde = messageSerde;
        this.messageFixedLength = messageFixedLength;
        this.fixedLength = newFixedLength(messageFixedLength);
    }

    public static int newFixedLength(final Integer messageFixedLength) {
        if (messageFixedLength == null || messageFixedLength < 0) {
            return ByteBuffers.EXPANDABLE_LENGTH;
        } else {
            return MESSAGE_INDEX + messageFixedLength;
        }
    }

    public ISerde<M> getMessageSerde() {
        return messageSerde;
    }

    public Integer getMessageFixedLength() {
        return messageFixedLength;
    }

    public Integer getFixedLength() {
        if (fixedLength < 0) {
            return null;
        } else {
            return fixedLength;
        }
    }

    @Override
    public IServiceSynchronousCommand<M> fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final IServiceSynchronousCommand<M> obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public IServiceSynchronousCommand<M> fromBuffer(final IByteBuffer buffer) {
        final int service = buffer.getInt(SERVICE_INDEX);
        final int method = buffer.getInt(METHOD_INDEX);
        final int sequence = buffer.getInt(SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - MESSAGE_INDEX;
        final M message = messageSerde.fromBuffer(buffer.slice(MESSAGE_INDEX, messageLength));
        return new ImmutableServiceSynchronousCommand<M>(service, method, sequence, message);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final IServiceSynchronousCommand<M> obj) {
        return obj.toBuffer(messageSerde, buffer);
    }

}
