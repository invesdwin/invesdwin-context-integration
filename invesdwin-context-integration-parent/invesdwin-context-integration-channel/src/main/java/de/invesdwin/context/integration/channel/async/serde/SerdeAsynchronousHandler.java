package de.invesdwin.context.integration.channel.async.serde;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class SerdeAsynchronousHandler<I, O>
        implements IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider>, IByteBufferProvider {

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
    public IByteBufferProvider open() throws IOException {
        output = delegate.open();
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider idle() throws IOException {
        output = delegate.idle();
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider handle(final IByteBufferProvider inputBuffer) throws IOException {
        final I input = inputSerde.fromBuffer(inputBuffer);
        output = delegate.handle(input);
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public void outputFinished() throws IOException {
        delegate.outputFinished();
        output = null;
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
    public int getBuffer(final IByteBuffer dst) {
        final int size = outputSerde.toBuffer(dst, output);
        output = null;
        return size;
    }

    @Override
    public IByteBuffer asBuffer() {
        if (outputBuffer == null) {
            //needs to be expandable so that FragmentSynchronousWriter can work properly
            outputBuffer = ByteBuffers.allocateExpandable(this.outputFixedLength);
        }
        final int length = getBuffer(outputBuffer);
        return outputBuffer.slice(0, length);
    }

}
