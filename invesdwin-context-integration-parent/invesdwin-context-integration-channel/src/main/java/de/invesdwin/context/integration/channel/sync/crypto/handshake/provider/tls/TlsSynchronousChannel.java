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
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.HandshakeValidation;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
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
    private final SynchronousReaderSpinWait<IByteBufferProvider> underlyingReaderSpinWait;
    private final SynchronousWriterSpinWait<IByteBufferProvider> underlyingWriterSpinWait;
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
    private volatile boolean rehandshaking = false;

    public TlsSynchronousChannel(final Duration handshakeTimeout, final Integer handshakeTimeoutRecoveryTries,
            final InetSocketAddress socketAddress, final ITlsProtocol protocol, final SSLEngine engine,
            final SynchronousReaderSpinWait<IByteBufferProvider> underlyingReaderSpinWait,
            final SynchronousWriterSpinWait<IByteBufferProvider> underlyingWriterSpinWait,
            final HandshakeValidation handshakeValidation) {
        this.handshakeTimeout = handshakeTimeout;
        this.handshakeTimeoutRecoveryTries = handshakeTimeoutRecoveryTries;
        this.socketAdddress = socketAddress;
        this.protocol = protocol;
        this.engine = engine;
        this.underlyingReaderSpinWait = underlyingReaderSpinWait;
        this.underlyingWriterSpinWait = underlyingWriterSpinWait;
        this.server = !engine.getUseClientMode();
        this.side = server ? "Server" : "Client";
        this.handshakeValidation = handshakeValidation;
    }

    public boolean writeReady() throws IOException {
        return underlyingWriterSpinWait.getWriter().writeReady();
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

        underlyingReaderSpinWait.getReader().open();
        underlyingWriterSpinWait.getWriter().open();

        outboundApplicationDataSize = java.nio.ByteBuffer.allocateDirect(Integer.BYTES);
        inboundApplicationData = inboundApplicationDataBuffer.asNioByteBuffer();
        // eliminates array creation on each call to SSLEngine.wrap()
        outboundApplicationDataArray = new java.nio.ByteBuffer[] { outboundApplicationDataSize, null };
        inboundApplicationDataArray = new java.nio.ByteBuffer[] { inboundApplicationData };

        performHandshake();
    }

    public boolean action() throws IOException {
        if (rehandshaking) {
            //wait for rehandshake to complete
            return false;
        }
        final HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs != HandshakeStatus.FINISHED && hs != HandshakeStatus.NOT_HANDSHAKING) {
            /*
             * someone (or the underlying implementation for e.g. a re-keying) has requested a rehandshake. Handle this
             * explicitly since the application might only use one direction of the channel. Thus wrap/unwrap would not
             * be called properly. Also for DTLS the handshaker handles the packet loss of the rehandshake. The
             * application only handles packet loss for application data. So another reason to use the handshaker here.
             */
            //don't rehandshake when we are already closing this channel
            if (inboundApplicationDataBuffer != null) {
                rehandshaking = true;
                if (!performHandshake()) {
                    return false;
                }
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

    /**
     * When used inside a non-blocking multiplexer this method should just throw an IOException instead of performing
     * the blocking handshake. Then let the client reconnect by itself to a separated acceptor thread in the
     * multiplexing server.
     * 
     * Alternatively override this method to perform the blocking handshake in a separate thread that performs
     * super.performHandshake(). Further action() calls to the channel will say that this channel has nothing to do for
     * the multiplexing thread. Once super.performHandshake() finishes, rehandshake is automatically set to true, which
     * informs the multiplexing thread that this channel again has some work to do (if there is work to do).
     * 
     * Though when performing the handshake asynchronously in a different thread, make sure the underlying transport
     * writer/channel is not used in other threads. Though since TLS is a stateful protocol, it might be hard to reuse
     * underlying transport writer/channel for multiple TLS sessions (Or why would one even want to do that? Normally
     * you have a TLS session per e.g. accepted socket client session. Only a non-wrapping gateway might have to handle
     * this differently.).
     */
    protected boolean performHandshake() throws IOException {
        final TlsHandshaker handshaker = TlsHandshakerObjectPool.INSTANCE.borrowObject();
        try {
            handshaker.init(handshakeTimeout, handshakeTimeoutRecoveryTries, socketAdddress, server, side, protocol,
                    engine, underlyingReaderSpinWait, underlyingWriterSpinWait, handshakeValidation);
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
            rehandshaking = false;
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
        if (inboundEncodedData == null && underlyingReaderSpinWait.getReader().hasNext()) {
            final IByteBuffer encodedMessage = underlyingReaderSpinWait.getReader().readMessage().asBuffer();
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
                underlyingReaderSpinWait.getReader().readFinished();
            }
        }
        return busy;
    }

    private boolean deliverAppData() throws IOException {
        if (outboundEncodedData == null) {
            throw FastEOFException.getInstance("closed");
        }
        if (!underlyingWriterSpinWait.getWriter().writeFlushed()) {
            //wait for last transmission to complete before sending more bytes through output buffer
            return true;
        }

        boolean busy = false;
        //drain unencrypted output
        if (outboundApplicationData != null) {
            if (!underlyingWriterSpinWait.getWriter().writeReady()) {
                //we have to wait for additional
                return true;
            }

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
            ByteBuffers.flip(outboundEncodedData);
            final IByteBuffer encodedMessage = outboundEncodedDataBuffer.slice(outboundEncodedData.position(),
                    outboundEncodedData.remaining());
            underlyingWriterSpinWait.getWriter().write(encodedMessage);
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

        underlyingWriterSpinWait.getWriter().close();
        underlyingReaderSpinWait.getReader().close();

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