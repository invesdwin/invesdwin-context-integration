package de.invesdwin.context.integration.channel.async.serde;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class SerdeAsynchronousHandlerFactory<I, O>
        implements IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> {

    private final IAsynchronousHandlerFactory<I, O> delegate;
    private final ISerde<I> inputSerde;
    private final ISerde<O> outputSerde;
    private final int outputFixedLength;

    public SerdeAsynchronousHandlerFactory(final IAsynchronousHandlerFactory<I, O> delegate, final ISerde<I> inputSerde,
            final ISerde<O> outputSerde, final int outputFixedLength) {
        this.delegate = delegate;
        this.inputSerde = inputSerde;
        this.outputSerde = outputSerde;
        this.outputFixedLength = outputFixedLength;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> newHandler() {
        return new SerdeAsynchronousHandler<>(delegate.newHandler(), inputSerde, outputSerde, outputFixedLength);
    }

}
