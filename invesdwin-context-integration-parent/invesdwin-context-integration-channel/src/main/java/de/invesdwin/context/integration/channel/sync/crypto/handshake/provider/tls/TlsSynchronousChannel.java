package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.HandshakeValidation;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.SslEngineStateMachine
 */
@NotThreadSafe
public class TlsSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private final Duration handshakeTimeout;
    private final Integer handshakeTimeoutRecoveryTries;
    private final InetSocketAddress socketAdddress;
    private final ITlsProtocol protocol;
    private final SSLEngine engine;
    private final ASpinWait readerSpinWait;
    private final ISynchronousReader<IByteBufferProvider> underlyingReader;
    private final ISynchronousWriter<IByteBufferProvider> underlyingWriter;
    private final boolean server;
    private final String side;
    private final HandshakeValidation handshakeValidation;

    private java.nio.ByteBuffer outboundApplicationDataSize;
    private java.nio.ByteBuffer outboundApplicationData;
    private IByteBuffer outboundEncodedDataBuffer;
    private java.nio.ByteBuffer outboundEncodedData;
    private java.nio.ByteBuffer inboundEncodedData;
    private IByteBuffer inboundApplicationDataBuffer;
    private java.nio.ByteBuffer inboundApplicationData;
    private java.nio.ByteBuffer[] outboundApplicationDataArray;
    private java.nio.ByteBuffer[] inboundApplicationDataArray;

    public TlsSynchronousChannel(final Duration handshakeTimeout, final Integer handshakeTimeoutRecoveryTries,
            final InetSocketAddress socketAddress, final ITlsProtocol protocol, final SSLEngine engine,
            final ASpinWait readerSpinWait, final ISynchronousReader<IByteBufferProvider> underlyingReader,
            final ISynchronousWriter<IByteBufferProvider> underlyingWriter,
            final HandshakeValidation handshakeValidation) {
        this.handshakeTimeout = handshakeTimeout;
        this.handshakeTimeoutRecoveryTries = handshakeTimeoutRecoveryTries;
        this.socketAdddress = socketAddress;
        this.protocol = protocol;
        this.engine = engine;
        this.readerSpinWait = readerSpinWait;
        this.underlyingReader = underlyingReader;
        this.underlyingWriter = underlyingWriter;
        this.server = !engine.getUseClientMode();
        this.side = server ? "Server" : "Client";
        this.handshakeValidation = handshakeValidation;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(side).toString();
    }

    public java.nio.ByteBuffer getInboundApplicationData() {
        return inboundApplicationData;
    }

    public IByteBuffer getInboundApplicationDataBuffer() {
        return inboundApplicationDataBuffer;
    }

    public void setOutboundApplicationDataBuffer(final IByteBuffer buffer) {
        if (outboundApplicationData != null) {
            throw new IllegalStateException("previous write did not finish");
        }
        outboundApplicationDataSize.clear();
        outboundApplicationDataSize.putInt(SIZE_INDEX, buffer.capacity());
        outboundApplicationData = buffer.asNioByteBuffer();
        outboundApplicationDataArray[1] = outboundApplicationData;
    }

    @Override
    public void open() throws IOException {
        synchronized (this) {
            if (inboundApplicationDataBuffer != null) {
                return;
            }
            inboundApplicationDataBuffer = ByteBuffers
                    .allocateDirectExpandable(engine.getSession().getApplicationBufferSize());
        }

        underlyingReader.open();
        underlyingWriter.open();

        outboundApplicationDataSize = java.nio.ByteBuffer.allocateDirect(Integer.BYTES);
        inboundApplicationData = inboundApplicationDataBuffer.asNioByteBuffer();
        // eliminates array creation on each call to SSLEngine.wrap()
        outboundApplicationDataArray = new java.nio.ByteBuffer[] { outboundApplicationDataSize, null };
        inboundApplicationDataArray = new java.nio.ByteBuffer[] { inboundApplicationData };

        performHandshake();
    }

    public boolean action() throws IOException {
        final HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs != HandshakeStatus.FINISHED && hs != HandshakeStatus.NOT_HANDSHAKING) {
            /*
             * someone (or the underlying implementation for e.g. a re-keying) has requested a rehandshake. Handle this
             * explicitly since the application might only use one direction of the channel. Thus wrap/unwrap would not
             * be called properly. Also for DTLS the handshaker handles the packet loss of the rehandshake. The
             * application only handles packet loss for application data. So another reason to use the handshaker here.
             */

            if (!performHandshake()) {
                return false;
            }
        }
        /*
         * we return directly because a channel will always be used by a single thread and he either writes or reads,
         * never both at the same time
         */
        if (receiveAppData()) {
            return true;
        }
        if (deliverAppData()) {
            return true;
        }
        return false;
    }

    private boolean performHandshake() throws IOException {
        final TlsHandshaker handshaker = TlsHandshakerObjectPool.INSTANCE.borrowObject();
        try {
            handshaker.init(handshakeTimeout, handshakeTimeoutRecoveryTries, socketAdddress, server, side, protocol,
                    engine, readerSpinWait, underlyingReader, underlyingWriter, handshakeValidation);
            handshaker.performHandshake();
            updateOutboundEncodedData();
            return true;
        } catch (final EOFException e) {
            return false;
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        } finally {
            TlsHandshakerObjectPool.INSTANCE.returnObject(handshaker);
        }
    }

    private void updateOutboundEncodedData() {
        if (outboundEncodedDataBuffer == null
                || outboundEncodedDataBuffer.capacity() < engine.getSession().getPacketBufferSize()) {
            //init outbound afterwards because we will encounter false buffer overflows if the required packet buffer size increased larger than the actual buffer
            outboundEncodedDataBuffer = ByteBuffers.allocateDirect(engine.getSession().getPacketBufferSize());
            outboundEncodedData = outboundEncodedDataBuffer.asNioByteBuffer();
        }
    }

    private boolean receiveAppData() throws IOException {
        boolean busy = false;

        //read encrypted data
        if (inboundEncodedData == null && underlyingReader.hasNext()) {
            final IByteBuffer encodedMessage = underlyingReader.readMessage().asBuffer();
            inboundEncodedData = encodedMessage.asNioByteBuffer();
        }

        //decrypt data and make it available to the reader
        if (inboundEncodedData != null) {
            final Status status = engine.unwrap(inboundEncodedData, inboundApplicationDataArray).getStatus();
            switch (status) {
            case BUFFER_UNDERFLOW:
                throw new IllegalStateException("underflow should not happen here");
            case BUFFER_OVERFLOW:
                final int positionBefore = inboundApplicationData.position();
                ByteBuffers.expand(inboundApplicationDataBuffer);
                inboundApplicationData = inboundApplicationDataBuffer.asNioByteBuffer();
                ByteBuffers.position(inboundApplicationData, positionBefore);
                inboundApplicationDataArray[0] = inboundApplicationData;
                break;
            case OK:
                break;
            case CLOSED:
                throw FastEOFException.getInstance("closed");
            default:
                throw UnknownArgumentException.newInstance(Status.class, status);
            }
            final boolean hasRemaining = inboundEncodedData.hasRemaining();
            busy |= hasRemaining;
            if (!hasRemaining) {
                inboundEncodedData.clear();
                inboundEncodedData = null;
                underlyingReader.readFinished();
            }
        }
        return busy;
    }

    private boolean deliverAppData() throws IOException {
        if (outboundEncodedData == null) {
            throw FastEOFException.getInstance("closed");
        }

        boolean busy = false;
        //drain unencrypted output
        if (outboundApplicationData != null) {
            final Status status = engine.wrap(outboundApplicationDataArray, outboundEncodedData).getStatus();
            switch (status) {
            case BUFFER_UNDERFLOW:
                throw new IllegalStateException("buffer underflow despite outboundApplicationData.position="
                        + outboundApplicationData.position());
            case BUFFER_OVERFLOW:
                //just send over the data chunk
                break;
            case OK:
                break;
            case CLOSED:
                throw FastEOFException.getInstance("closed");
            default:
                throw UnknownArgumentException.newInstance(Status.class, status);
            }
            final boolean hasRemaining = outboundApplicationData.hasRemaining();
            busy |= hasRemaining;
            if (!hasRemaining) {
                outboundApplicationData.clear();
                outboundApplicationData = null;
                outboundApplicationDataArray[1] = null;
            }
        }
        //send encrypted output
        if (outboundEncodedData.position() != 0) {
            outboundEncodedData.flip();
            final IByteBuffer encodedMessage = outboundEncodedDataBuffer.slice(outboundEncodedData.position(),
                    outboundEncodedData.remaining());
            underlyingWriter.write(encodedMessage);
            //CHECKSTYLE:OFF
            while (!underlyingWriter.writeFinished()) {
                //CHECKSTYLE:ON
                //System.out.println("TODO: integrate into outer loop");
                //repeat
            }
            outboundEncodedData.clear();
        }
        return busy;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (inboundApplicationDataBuffer == null) {
                return;
            }
            inboundApplicationDataBuffer = null;
        }

        final long startNanos = System.nanoTime();
        engine.closeOutbound();
        try {
            //signal close to the other side for faster exit
            while (action()) {
                try {
                    SynchronousChannels.DEFAULT_WAIT_INTERVAL.sleep();
                } catch (final InterruptedException e) {
                    throw new IOException(e);
                }
                if (handshakeTimeout.isLessThanNanos(System.nanoTime() - startNanos)) {
                    throw new TimeoutException("Close handshake timeout exceeded");
                }
            }
        } catch (final EOFException e) {
            //ignore
        } catch (final IOException e) {
            throw e;
        } catch (final TimeoutException e) {
            throw new IOException(e);
        }
        //        try {
        //            engine.closeInbound();
        //        } catch (final SSLException e) {
        //            throw new IOException(e);
        //        }

        underlyingWriter.close();
        underlyingReader.close();

        outboundEncodedDataBuffer = null;
        outboundApplicationDataSize = null;
        outboundApplicationData = null;
        outboundEncodedData = null;
        inboundApplicationData = null;
        inboundEncodedData = null;
        outboundApplicationDataArray = null;
        inboundApplicationDataArray = null;
    }
}