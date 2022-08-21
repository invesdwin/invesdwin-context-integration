package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.net.SocketAddress;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;
import de.invesdwin.util.time.duration.Duration;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.Handshaker
 * 
 * Debug with : -Djavax.net.debug=all
 */
@NotThreadSafe
public class TlsHandshaker {
    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;
    private static final Log LOG = new Log(TlsHandshaker.class);

    private final java.nio.ByteBuffer applicationData;
    private final java.nio.ByteBuffer networkData;
    private final java.nio.ByteBuffer peerApplicationData;
    private final java.nio.ByteBuffer peerNetworkData;
    private final UnsafeByteBuffer byteBufferWrapper;

    public TlsHandshaker() {
        this.applicationData = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.networkData = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.peerApplicationData = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.peerNetworkData = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
        this.byteBufferWrapper = new UnsafeByteBuffer();
    }

    //CHECKSTYLE:OFF
    public void performHandshake(final Duration handshakeTimeout, final SocketAddress address, final SSLEngine engine,
            final ASpinWait readerSpinWait, final ISynchronousReader<IByteBuffer> reader,
            final ISynchronousWriter<IByteBufferWriter> writer) throws Exception {
        //CHECKSTYLE:ON

        final String side = engine.getUseClientMode() ? "client" : "server";
        LOG.debug("%s:%s beginning handshake", address, side);
        engine.beginHandshake();

        HandshakeStatus status = engine.getHandshakeStatus();
        SSLEngineResult result;

        long underflowCount = 0;
        HandshakeStatus lastStatus = status;
        LOG.debug("%s:%s initial status %s", address, side, status);

        while (status != HandshakeStatus.FINISHED && status != HandshakeStatus.NOT_HANDSHAKING) {
            switch (status) {
            case NEED_UNWRAP:
                if (!readerSpinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                    throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
                }
                final IByteBuffer message = reader.readMessage();
                //this additional copy here could be removed, but for the handshake it should not matter much
                message.getBytesTo(0, peerNetworkData, message.capacity());
                ByteBuffers.position(peerNetworkData, message.capacity());
                reader.readFinished();
                if (message.capacity() == 0 && (peerNetworkData.remaining() == 0
                        || peerNetworkData.remaining() == peerNetworkData.capacity())) {
                    underflowCount++;
                    continue;
                }
                peerNetworkData.flip();
                final int dataReceived = peerNetworkData.remaining();
                LOG.debug("%s:%s Received %s from handshake peer", address, side, dataReceived);
                result = engine.unwrap(peerNetworkData, peerApplicationData);
                peerNetworkData.compact();
                switch (result.getStatus()) {
                case OK:
                    break;
                case BUFFER_UNDERFLOW:
                    if ((underflowCount & 65535L) == 0L) {
                        LOG.debug("Not enough data read from remote end (%s)", dataReceived);
                    }
                    underflowCount++;
                    break;
                default:
                    LOG.error("Bad handshake status: %s/%s", result.getStatus(), result.getHandshakeStatus());
                    break;
                }
                break;
            case NEED_WRAP:
                networkData.clear();
                result = engine.wrap(applicationData, networkData);

                switch (result.getStatus()) {
                case OK:
                    networkData.flip();
                    final int remaining = networkData.remaining();
                    if (remaining > 0) {
                        final int position = networkData.position();
                        byteBufferWrapper.wrap(networkData, position, remaining);
                        writer.write(byteBufferWrapper);
                        ByteBuffers.position(networkData, position + remaining);
                    }
                    LOG.debug("%s:%s Wrote %s to handshake peer", address, side, remaining);
                    break;
                default:
                    throw new UnsupportedOperationException(result.getStatus().toString());
                }
                break;
            case NEED_TASK:
                Runnable delegatedTask;
                while ((delegatedTask = engine.getDelegatedTask()) != null) {
                    try {
                        delegatedTask.run();
                        LOG.debug("Ran task %s", delegatedTask);
                    } catch (final RuntimeException e) {
                        LOG.error("Delegated task threw exception", e);
                    }
                }
                break;
            default:
                throw UnknownArgumentException.newInstance(HandshakeStatus.class, status);
            }

            status = engine.getHandshakeStatus();
            if (status != lastStatus) {
                LOG.debug("%s:%s status change to %s", address, side, status);
                lastStatus = status;
            }
        }
    }

}
