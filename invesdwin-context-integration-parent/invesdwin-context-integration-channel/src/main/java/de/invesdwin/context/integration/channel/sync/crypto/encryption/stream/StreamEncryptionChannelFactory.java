package de.invesdwin.context.integration.channel.sync.crypto.encryption.stream;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@Immutable
public class StreamEncryptionChannelFactory implements ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> {

    private final IEncryptionFactory encryptionFactory;

    public StreamEncryptionChannelFactory(final IEncryptionFactory encryptionFactory) {
        this.encryptionFactory = encryptionFactory;
    }

    @Override
    public ISynchronousReader<IByteBuffer> newReader(final ISynchronousReader<IByteBuffer> reader) {
        return new StreamEncryptionSynchronousReader(reader, encryptionFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferWriter> newWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new StreamEncryptionSynchronousWriter(writer, encryptionFactory);
    }

}
