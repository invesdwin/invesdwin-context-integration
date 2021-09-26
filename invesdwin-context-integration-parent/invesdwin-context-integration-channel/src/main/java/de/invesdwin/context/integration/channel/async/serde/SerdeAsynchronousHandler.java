package de.invesdwin.context.integration.channel.async.serde;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class SerdeAsynchronousHandler<I, O>
        implements IAsynchronousHandler<IByteBuffer, IByteBufferWriter>, IByteBufferWriter {

    private final IAsynchronousHandler<I, O> delegate;
    private final ISerde<I> inputSerde;
    private final ISerde<O> outputSerde;
    private final int outputFixedLength;

    private O output;
    private IByteBuffer outputBuffer;

    public SerdeAsynchronousHandler(final IAsynchronousHandler<I, O> delegate, final ISerde<I> inputSerde,
            final ISerde<O> outputSerde, final int outputFixedLength) {
        this.delegate = delegate;
        this.inputSerde = inputSerde;
        this.outputSerde = outputSerde;
        this.outputFixedLength = ByteBuffers.newAllocateFixedLength(outputFixedLength);
    }

    @Override
    public IByteBufferWriter open() throws IOException {
        output = delegate.open();
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferWriter handle(final IByteBuffer inputBuffer) throws IOException {
        final I input = inputSerde.fromBuffer(inputBuffer, inputBuffer.capacity());
        output = delegate.handle(input);
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        output = null;
        outputBuffer = null;
        try {
            delegate.close();
        } catch (final IOException e) {
            //ignore
        }
    }

    @Override
    public int write(final IByteBuffer buffer) {
        final int size = outputSerde.toBuffer(buffer, output);
        output = null;
        return size;
    }

    @Override
    public IByteBuffer asBuffer() {
        if (outputBuffer == null) {
            outputBuffer = ByteBuffers.allocate(this.outputFixedLength);
        }
        final int length = write(outputBuffer);
        return outputBuffer.slice(0, length);
    }

}
