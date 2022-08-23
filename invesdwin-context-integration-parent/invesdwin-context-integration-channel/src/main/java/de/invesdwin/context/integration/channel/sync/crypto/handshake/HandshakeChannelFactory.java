package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Usage is:
 * 
 * <ol>
 * <li>Create reader and writer, they will be connected to each other. More reader/writer pairs can be created in
 * succession. Don't create multiple reader in a row or multiple writers in a row.</li>
 * <li>Open reader and writer, they will perform the handshake. If you try to send/receive messages beforehand, an
 * exception will be thrown that tells that both sides need to be opened.</li>
 * <li>Now you can send/receive messages. The handshake is already complete.</li>
 * <li>Reader/Writer can be closed individually as with any other synchronous channels.</li>
 * </ol>
 * 
 * This process/contract allows code to use the handshake factory without any modification. Just add it as a wrapper to
 * the existing code that follows this process and it looks like any other factory is being used. DisabledChannelFactory
 * can be used to disable the handshake.
 */
@NotThreadSafe
public class HandshakeChannelFactory implements ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> {

    private final IHandshakeProvider handshakeProvider;
    private HandshakeChannel handshakeChannel;

    public HandshakeChannelFactory(final IHandshakeProvider handshakeProvider) {
        this.handshakeProvider = handshakeProvider;
    }

    @Override
    public ISynchronousReader<IByteBuffer> newReader(final ISynchronousReader<IByteBuffer> reader) {
        if (handshakeChannel == null) {
            handshakeChannel = new HandshakeChannel(handshakeProvider);
        }
        final HandshakeSynchronousReader handshakeReader = handshakeChannel.getReader();
        handshakeReader.setUnderlyingReader(reader);
        if (handshakeChannel.getReader().getUnderlyingReader() != null
                && handshakeChannel.getWriter().getUnderlyingWriter() != null) {
            //start with the next handshake connection
            handshakeChannel = null;
        }
        return handshakeReader;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        if (handshakeChannel == null) {
            handshakeChannel = new HandshakeChannel(handshakeProvider);
        }
        final HandshakeSynchronousWriter handshakeWriter = handshakeChannel.getWriter();
        handshakeWriter.setUnderlyingWriter(writer);
        if (handshakeChannel.getReader().getUnderlyingReader() != null
                && handshakeChannel.getWriter().getUnderlyingWriter() != null) {
            //start with the next handshake connection
            handshakeChannel = null;
        }
        return handshakeWriter;
    }

}
