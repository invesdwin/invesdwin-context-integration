package de.invesdwin.context.integration.channel.sync.command;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.SerdeBaseMethods;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@Immutable
public class SynchronousCommandSerde<M> implements ISerde<ISynchronousCommand<M>> {

    public static final int TYPE_INDEX = 0;
    public static final int TYPE_SIZE = Integer.BYTES;

    public static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    public static final int SEQUENCE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private final ISerde<M> messageSerde;
    private final Integer messageFixedLength;
    private final int fixedLength;

    public SynchronousCommandSerde(final ISerde<M> messageSerde, final Integer messageFixedLength) {
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
    public ISynchronousCommand<M> fromBytes(final byte[] bytes) {
        return SerdeBaseMethods.fromBytes(this, bytes);
    }

    @Override
    public byte[] toBytes(final ISynchronousCommand<M> obj) {
        return SerdeBaseMethods.toBytes(this, obj);
    }

    @Override
    public ISynchronousCommand<M> fromBuffer(final IByteBuffer buffer) {
        final int type = buffer.getInt(TYPE_INDEX);
        final int sequence = buffer.getInt(SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - MESSAGE_INDEX;
        final M message = messageSerde.fromBuffer(buffer.slice(MESSAGE_INDEX, messageLength));
        return new ImmutableSynchronousCommand<M>(type, sequence, message);
    }

    @Override
    public int toBuffer(final IByteBuffer buffer, final ISynchronousCommand<M> obj) {
        buffer.putInt(TYPE_INDEX, obj.getType());
        buffer.putInt(SEQUENCE_INDEX, obj.getSequence());
        final int messageLength = messageSerde.toBuffer(buffer.sliceFrom(MESSAGE_INDEX), obj.getMessage());
        return MESSAGE_INDEX + messageLength;
    }

}
