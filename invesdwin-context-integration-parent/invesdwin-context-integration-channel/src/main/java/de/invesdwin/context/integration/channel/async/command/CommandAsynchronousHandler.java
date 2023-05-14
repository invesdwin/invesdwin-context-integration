package de.invesdwin.context.integration.channel.async.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.sync.command.MutableSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.command.SynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

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
    public IByteBufferProvider handle(final IByteBuffer inputBuffer) throws IOException {
        final int type = inputBuffer.getInt(SynchronousCommandSerde.TYPE_INDEX);
        final int sequence = inputBuffer.getInt(SynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = inputBuffer.capacity() - SynchronousCommandSerde.MESSAGE_INDEX;
        final I input = inputSerde.fromBuffer(inputBuffer.slice(SynchronousCommandSerde.MESSAGE_INDEX, messageLength));
        inputCommand.setType(type);
        inputCommand.setSequence(sequence);
        inputCommand.setMessage(input);
        output = delegate.handle(inputCommand);
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

}
