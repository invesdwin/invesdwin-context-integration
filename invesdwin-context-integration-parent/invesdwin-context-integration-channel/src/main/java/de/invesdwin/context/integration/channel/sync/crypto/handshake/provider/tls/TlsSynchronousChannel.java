package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
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
    private final SSLEngine engine;
    private final ISynchronousReader<IByteBuffer> underlyingReader;
    private final ISynchronousWriter<IByteBufferWriter> underlyingWriter;

    private IByteBuffer outboundApplicationDataBuffer;
    private java.nio.ByteBuffer outboundApplicationData;
    private java.nio.ByteBuffer outboundEncodedData;
    private java.nio.ByteBuffer inboundEncodedData;
    private IByteBuffer inboundApplicationDataBuffer;
    private java.nio.ByteBuffer inboundApplicationData;
    private java.nio.ByteBuffer[] outboundApplicationDataArray;
    private java.nio.ByteBuffer[] inboundApplicationDataArray;

    public TlsSynchronousChannel(final Duration handshakeTimeout, final SSLEngine engine,
            final ISynchronousReader<IByteBuffer> underlyingReader,
            final ISynchronousWriter<IByteBufferWriter> underlyingWriter) {
        this.handshakeTimeout = handshakeTimeout;
        this.engine = engine;
        this.underlyingReader = underlyingReader;
        this.underlyingWriter = underlyingWriter;

    }

    public java.nio.ByteBuffer getInboundApplicationData() {
        return inboundApplicationData;
    }

    public IByteBuffer getInboundApplicationDataBuffer() {
        return inboundApplicationDataBuffer;
    }

    public java.nio.ByteBuffer getOutboundApplicationData() {
        return outboundApplicationData;
    }

    public IByteBuffer getOutboundApplicationDataBuffer() {
        return outboundApplicationDataBuffer;
    }

    @Override
    public void open() throws IOException {
        synchronized (this) {
            if (outboundApplicationDataBuffer != null) {
                return;
            }
            outboundApplicationDataBuffer = ByteBuffers
                    .allocateDirectExpandable(engine.getSession().getApplicationBufferSize());
            inboundApplicationDataBuffer = ByteBuffers
                    .allocateDirectExpandable(engine.getSession().getApplicationBufferSize());
        }

        underlyingReader.open();
        underlyingWriter.open();

        outboundApplicationData = outboundApplicationDataBuffer.asNioByteBuffer();
        outboundEncodedData = java.nio.ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        inboundApplicationData = inboundApplicationDataBuffer.asNioByteBuffer();
        inboundEncodedData = java.nio.ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        // eliminates array creation on each call to SSLEngine.wrap()
        outboundApplicationDataArray = new java.nio.ByteBuffer[] { outboundApplicationData };
        inboundApplicationDataArray = new java.nio.ByteBuffer[] { inboundApplicationData };
    }

    public boolean action() throws EOFException {
        final int read;
        final boolean busy = false;
        //        bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
        //        try {
        //            if (outboundApplicationData.position() != 0) {
        //
        //                outboundApplicationData.flip();
        //
        //                if (engine.wrap(precomputedWrapArray, outboundEncodedData)
        //                        .getStatus() == SSLEngineResult.Status.CLOSED) {
        //                    throw FastEOFException.getInstance("Socket closed");
        //                }
        //                busy = outboundApplicationData.hasRemaining();
        //                outboundApplicationData.compact();
        //            }
        //            if (outboundEncodedData.position() != 0) {
        //                outboundEncodedData.flip();
        //                bufferHandler.writeData(outboundEncodedData);
        //                busy |= outboundEncodedData.hasRemaining();
        //                outboundEncodedData.compact();
        //            }
        //
        //            read = bufferHandler.readData(inboundEncodedData);
        //            if (read == -1) {
        //                throw new IORuntimeException("Socket closed");
        //            }
        //            busy |= read != 0;
        //
        //            if (inboundEncodedData.position() != 0) {
        //                inboundEncodedData.flip();
        //                engine.unwrap(inboundEncodedData, precomputedUnwrapArray);
        //                busy |= inboundEncodedData.hasRemaining();
        //                inboundEncodedData.compact();
        //            }
        //
        //            if (inboundApplicationData.position() != 0) {
        //                inboundApplicationData.flip();
        //                bufferHandler.handleDecryptedData(inboundApplicationData, outboundApplicationData);
        //                busy |= inboundApplicationData.hasRemaining();
        //                inboundApplicationData.compact();
        //            }
        //        } catch (final IOException e) {
        //            throw new UncheckedIOException(e);
        //        }

        return busy;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (outboundApplicationDataBuffer == null) {
                return;
            }
            outboundApplicationDataBuffer = null;
            inboundApplicationDataBuffer = null;
        }

        final long startNanos = System.nanoTime();
        engine.closeOutbound();
        try {
            while (!engine.isOutboundDone()) {
                action();
                try {
                    SynchronousChannels.DEFAULT_WAIT_INTERVAL.sleep();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (handshakeTimeout.isLessThanNanos(System.nanoTime() - startNanos)) {
                    throw new TimeoutException("Close handshake timeout exceeded");
                }
            }
        } catch (final EOFException e) {
            //ignore
        } catch (final TimeoutException e) {
            throw new RuntimeException(e);
        }
        try {
            engine.closeInbound();
        } catch (final SSLException e) {
            throw new RuntimeException(e);
        }

        underlyingWriter.close();
        underlyingReader.close();

        outboundApplicationData = null;
        outboundEncodedData = null;
        inboundApplicationData = null;
        inboundEncodedData = null;
        outboundApplicationDataArray = null;
        inboundApplicationDataArray = null;
    }
}