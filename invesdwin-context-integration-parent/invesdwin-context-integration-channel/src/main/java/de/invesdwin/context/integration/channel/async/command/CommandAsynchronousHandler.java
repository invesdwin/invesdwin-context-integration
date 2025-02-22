package de.invesdwin.context.integration.channel.async.command;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.sync.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.sync.command.MutableSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.command.SynchronousCommandSerde;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;

@NotThreadSafe
public class CommandAsynchronousHandler<I, O>
        implements IAsynchronousHandler<IByteBuffer, IByteBufferProvider>, IByteBufferProvider {

    private final IAsynchronousHandler<ISynchronousCommand<I>, ISynchronousCommand<O>> delegate;
    private final ISerde<I> inputSerde;
    private final ISerde<O> outputSerde;
    private final int outputFixedLength;

    private ISynchronousCommand<O> output;
    private IByteBuffer outputBuffer;
    private final MutableSynchronousCommand<I> inputCommand = new MutableSynchronousCommand<>();

    public CommandAsynchronousHandler(
            final IAsynchronousHandler<ISynchronousCommand<I>, ISynchronousCommand<O>> delegate,
            final ISerde<I> inputSerde, final ISerde<O> outputSerde, final int outputFixedLength) {
        this.delegate = delegate;
        this.inputSerde = inputSerde;
        this.outputSerde = outputSerde;
        this.outputFixedLength = ByteBuffers.newAllocateFixedLength(outputFixedLength);
    }

    private CommandAsynchronousContext<O> commandContext(
            final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        return context.getAttributes()
                .getOrCreate(CommandAsynchronousContext.class.getSimpleName(),
                        () -> new CommandAsynchronousContext<>(context, outputSerde));
    }

    @Override
    public IByteBufferProvider open(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        output = delegate.open(commandContext(context));
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider idle(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        output = delegate.idle(commandContext(context));
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public IByteBufferProvider handle(final IAsynchronousHandlerContext<IByteBufferProvider> context,
            final IByteBuffer inputBuffer) throws IOException {
        final int type = inputBuffer.getInt(SynchronousCommandSerde.TYPE_INDEX);
        final int sequence = inputBuffer.getInt(SynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = inputBuffer.capacity() - SynchronousCommandSerde.MESSAGE_INDEX;
        final I input = inputSerde.fromBuffer(inputBuffer.slice(SynchronousCommandSerde.MESSAGE_INDEX, messageLength));
        inputCommand.setType(type);
        inputCommand.setSequence(sequence);
        inputCommand.setMessage(input);
        output = delegate.handle(commandContext(context), inputCommand);
        if (output != null) {
            return this;
        } else {
            return null;
        }
    }

    @Override
    public void outputFinished(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        delegate.outputFinished(commandContext(context));
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
        dst.putInt(SynchronousCommandSerde.TYPE_INDEX, output.getType());
        dst.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, output.getSequence());
        final int messageLength = outputSerde.toBuffer(dst.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                output.getMessage());
        final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        return length;
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

    private static final class CommandAsynchronousContext<O>
            implements IAsynchronousHandlerContext<ISynchronousCommand<O>> {

        private final IAsynchronousHandlerContext<IByteBufferProvider> delegate;
        private final ISerde<O> outputSerde;

        private CommandAsynchronousContext(final IAsynchronousHandlerContext<IByteBufferProvider> delegate,
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
        public Future<?> write(final ISynchronousCommand<O> output) {
            try (ICloseableByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject()) {
                buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, output.getType());
                buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, output.getSequence());
                final int messageLength = outputSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                        output.getMessage());
                final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
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
        public IAsynchronousHandlerContext<ISynchronousCommand<O>> asImmutable() {
            final IAsynchronousHandlerContext<IByteBufferProvider> immutable = delegate.asImmutable();
            if (immutable == delegate) {
                return this;
            } else {
                return new CommandAsynchronousContext<>(immutable, outputSerde);
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(delegate).toString();
        }

    }

}
