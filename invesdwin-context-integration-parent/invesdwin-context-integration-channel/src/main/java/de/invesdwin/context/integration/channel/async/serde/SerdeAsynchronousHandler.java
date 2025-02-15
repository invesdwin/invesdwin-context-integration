package de.invesdwin.context.integration.channel.async.serde;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;

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

    private SerdeAsynchronousContext<O> serdeContext(final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        return context.getAttributes()
                .getOrCreate(SerdeAsynchronousContext.class.getSimpleName(),
                        () -> new SerdeAsynchronousContext<>(context, outputSerde));
    }

    @Override
    public IByteBufferProvider open(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        output = delegate.open(serdeContext(context));
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider idle(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        output = delegate.idle(serdeContext(context));
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider handle(final IAsynchronousHandlerContext<IByteBufferProvider> context,
            final IByteBufferProvider inputBuffer) throws IOException {
        final I input = inputSerde.fromBuffer(inputBuffer);
        output = delegate.handle(serdeContext(context), input);
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public void outputFinished(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        delegate.outputFinished(serdeContext(context));
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

    private static final class SerdeAsynchronousContext<O> implements IAsynchronousHandlerContext<O> {

        private final IAsynchronousHandlerContext<IByteBufferProvider> delegate;
        private final ISerde<O> outputSerde;

        private SerdeAsynchronousContext(final IAsynchronousHandlerContext<IByteBufferProvider> delegate,
                final ISerde<O> outputSerde) {
            this.delegate = delegate;
            this.outputSerde = outputSerde;
        }

        @Override
        public String getSessionId() {
            return delegate.getSessionId();
        }

        @Override
        public AttributesMap getAttributes() {
            return delegate.getAttributes();
        }

        @Override
        public Future<?> write(final O output) {
            try (ICloseableByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject()) {
                final int length = outputSerde.toBuffer(buffer, output);
                return delegate.write(buffer.sliceTo(length));
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public ProcessResponseResult borrowResult() {
            return delegate.borrowResult();
        }

        @Override
        public void returnResult(final ProcessResponseResult result) {
            delegate.returnResult(result);
        }

        @Override
        public boolean registerCloseable(final Closeable closeable) {
            return delegate.registerCloseable(closeable);
        }

        @Override
        public boolean unregisterCloseable(final Closeable closeable) {
            return delegate.unregisterCloseable(closeable);
        }

        @Override
        public IAsynchronousHandlerContext<O> asImmutable() {
            final IAsynchronousHandlerContext<IByteBufferProvider> immutable = delegate.asImmutable();
            if (immutable == delegate) {
                return this;
            } else {
                return new SerdeAsynchronousContext<>(immutable, outputSerde);
            }
        }

    }

}
