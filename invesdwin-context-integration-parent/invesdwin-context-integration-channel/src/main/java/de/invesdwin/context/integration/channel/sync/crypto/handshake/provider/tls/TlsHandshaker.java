package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLSession;

import org.apache.commons.io.HexDump;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.HandshakeValidation;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.time.duration.Duration;

/**
 * Adapted from:
 * https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/master/test/jdk/javax/net/ssl/DTLS/DTLSOverDatagram.java
 * 
 * It also handles handshake timeouts and packet loss when DTLS is used with recovery.
 * 
 * Similar: net.openhft.chronicle.network.ssl.Handshaker
 * 
 * Debug with: -Djavax.net.debug=all
 */
@NotThreadSafe
public class TlsHandshaker {
    private static final Log LOG = new Log(TlsHandshaker.class);

    private static final int HANDSHAKE_BUFFER_CAPACITY = 32768;
    private static final int MAX_HANDSHAKE_LOOPS = 200;
    private static final int MAX_PRODUCE_HANDSHAKE_PACKETS_LOOPS = 100;
    private static final int MAX_APP_READ_LOOPS = 60;

    private final java.nio.ByteBuffer handshakeBuffer;

    private boolean server;
    private String side;

    private Duration handshakeTimeout;
    private SocketAddress address;
    private ITlsProtocol protocol;
    private SSLEngine engine;
    private ASpinWait readerSpinWait;
    private ISynchronousReader<IByteBufferProvider> reader;
    private ISynchronousWriter<IByteBufferProvider> writer;

    private HandshakeValidation handshakeValidation;

    public TlsHandshaker() {
        this.handshakeBuffer = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
    }

    public void init(final Duration handshakeTimeout, final SocketAddress address, final boolean server,
            final String side, final ITlsProtocol protocol, final SSLEngine engine, final ASpinWait readerSpinWait,
            final ISynchronousReader<IByteBufferProvider> reader, final ISynchronousWriter<IByteBufferProvider> writer,
            final HandshakeValidation handshakeValidation) {
        this.server = server;
        this.side = side;

        this.handshakeTimeout = handshakeTimeout;
        this.address = address;
        this.protocol = protocol;
        this.engine = engine;
        this.readerSpinWait = readerSpinWait;
        this.reader = reader;
        this.writer = writer;

        this.handshakeValidation = handshakeValidation;
    }

    public void reset() {
        this.server = false;
        this.side = null;

        this.handshakeTimeout = null;
        this.address = null;
        this.protocol = null;
        this.engine = null;
        this.readerSpinWait = null;
        this.reader = null;
        this.writer = null;

        handshakeValidation = null;

        this.handshakeBuffer.clear();
    }

    //CHECKSTYLE:OFF
    public void performHandshake() throws IOException, TimeoutException {
        //CHECKSTYLE:ON
        boolean endLoops = false;
        int loops = MAX_HANDSHAKE_LOOPS;
        HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == HandshakeStatus.FINISHED || hs == HandshakeStatus.NOT_HANDSHAKING) {
            engine.beginHandshake();
        }
        while (!endLoops) {

            if (--loops < 0) {
                throw new IOException("Too many loops to produce handshake packets");
            }

            hs = engine.getHandshakeStatus();
            if (LOG.isDebugEnabled()) {
                debug("%s: %s: =======handshake(%s, %s)=======", address, side, loops, hs);
            }
            if (hs == HandshakeStatus.NEED_UNWRAP || hs == HandshakeStatus.NEED_UNWRAP_AGAIN) {

                if (LOG.isDebugEnabled()) {
                    debug("%s: %s: Receive %s records, handshake status is %s", address, side, protocol.getFamily(),
                            hs);
                }

                final boolean readFinishedRequired;
                final java.nio.ByteBuffer iNet;
                final java.nio.ByteBuffer iApp;
                if (hs == HandshakeStatus.NEED_UNWRAP) {
                    try {
                        if (!readerSpinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                            if (protocol.isHandshakeTimeoutRecoveryEnabled()) {
                                //CHECKSTYLE:OFF
                                if (LOG.isDebugEnabled()) {
                                    //CHECKSTYLE:ON
                                    debug("%s: %s: Warning: Trying to recover from read handshake message timeout exceeded: %s",
                                            address, side, handshakeTimeout);
                                }

                                final boolean finished = onReceiveTimeout();
                                //CHECKSTYLE:OFF
                                if (finished) {
                                    if (LOG.isDebugEnabled()) {
                                        //CHECKSTYLE:ON
                                        debug("%s: %s: Handshake status is FINISHED after calling onReceiveTimeout(), finish the loop",
                                                address, side);
                                    }
                                    endLoops = true;
                                }

                                //CHECKSTYLE:OFF
                                if (LOG.isDebugEnabled()) {
                                    //CHECKSTYLE:ON
                                    debug("%s: %s: New handshake status is %s", address, side,
                                            engine.getHandshakeStatus());
                                }

                                continue;
                            } else {
                                throw new TimeoutException(
                                        "Read handshake message timeout exceeded: " + handshakeTimeout);
                            }
                        }
                    } catch (final IOException e) {
                        throw e;
                    } catch (final TimeoutException e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                    final IByteBuffer message = reader.readMessage().asBuffer();
                    handshakeBuffer.clear();
                    iNet = message.asNioByteBuffer();
                    iApp = handshakeBuffer;
                    readFinishedRequired = true;
                } else {
                    handshakeBuffer.clear();
                    iNet = EmptyByteBuffer.EMPTY_DIRECT_BYTE_BUFFER;
                    iApp = handshakeBuffer;
                    readFinishedRequired = false;
                }

                final SSLEngineResult r = engine.unwrap(iNet, iApp);
                if (readFinishedRequired) {
                    reader.readFinished();
                }
                final Status rs = r.getStatus();
                hs = r.getHandshakeStatus();
                //CHECKSTYLE:OFF
                if (rs == Status.OK) {
                    //CHECKSTYLE:ON
                    // OK
                } else if (rs == Status.BUFFER_OVERFLOW) {
                    if (LOG.isDebugEnabled()) {
                        debug("%s: %s: BUFFER_OVERFLOW, handshake status is %s", address, side, hs);
                    }

                    // the client maximum fragment size config does not work?
                    throw new IOException("Buffer overflow: incorrect client maximum fragment size");
                } else if (rs == Status.BUFFER_UNDERFLOW) {
                    if (LOG.isDebugEnabled()) {
                        debug("%s: %s: BUFFER_UNDERFLOW, handshake status is %s", address, side, hs);
                    }

                    // bad packet, or the client maximum fragment size
                    // config does not work?
                    if (hs != HandshakeStatus.NOT_HANDSHAKING && hs != HandshakeStatus.NEED_UNWRAP) {
                        throw new IOException("Buffer underflow: incorrect client maximum fragment size");
                    } // otherwise, ignore this packet
                } else if (rs == Status.CLOSED) {
                    throw FastEOFException.getInstance("SSL engine closed, handshake status is " + hs);
                } else {
                    throw new IOException("Can't reach here, result is " + rs);
                }

                if (hs == HandshakeStatus.FINISHED) {
                    if (LOG.isDebugEnabled()) {
                        debug("%s: %s: Handshake status is FINISHED, finish the loop", address, side);
                    }
                    endLoops = true;
                }
            } else if (hs == HandshakeStatus.NEED_WRAP) {
                final boolean finished = produceHandshakePackets("Produced");
                if (finished) {
                    if (LOG.isDebugEnabled()) {
                        debug("%s: %s: Handshake status is FINISHED after producing handshake packets, finish the loop",
                                address, side);
                    }
                    endLoops = true;
                }
            } else if (hs == HandshakeStatus.NEED_TASK) {
                runDelegatedTasks(engine);
            } else if (hs == HandshakeStatus.NOT_HANDSHAKING) {
                if (LOG.isDebugEnabled()) {
                    debug("%s: %s: Handshake status is NOT_HANDSHAKING, finish the loop", address, side);
                }
                endLoops = true;
            } else if (hs == HandshakeStatus.FINISHED) {
                throw new IOException("Unexpected status, SSLEngine.getHandshakeStatus() shouldn't return FINISHED");
            } else {
                throw new IOException("Can't reach here, handshake status is " + hs);
            }
        }

        hs = engine.getHandshakeStatus();
        if (LOG.isDebugEnabled()) {
            debug("%s: %s: Handshake finished, status is %s", address, side, hs);
        }

        if (engine.getHandshakeSession() != null) {
            throw new IOException("Handshake finished, but handshake session is not null");
        }

        final SSLSession session = engine.getSession();
        if (session == null) {
            throw new IOException("Handshake finished, but session is null");
        }
        if (LOG.isDebugEnabled()) {
            debug("%s: %s: Negotiated protocol is %s", address, side, session.getProtocol());
            debug("%s: %s: Negotiated cipher suite is %s", address, side, session.getCipherSuite());
        }

        // handshake status should be NOT_HANDSHAKING
        //
        // According to the spec, SSLEngine.getHandshakeStatus() can't
        // return FINISHED.
        if (hs != HandshakeStatus.NOT_HANDSHAKING) {
            throw new IOException("Unexpected handshake status " + hs);
        }

        if (handshakeValidation != null) {
            if (server) {
                // read client application data
                receiveAppData(handshakeValidation.getClientPayload().asNioByteBuffer());
                // write server application data
                deliverAppData(handshakeValidation.getServerPayload().asNioByteBuffer());
            } else {
                // write client application data
                deliverAppData(handshakeValidation.getClientPayload().asNioByteBuffer());
                // read server application data
                receiveAppData(handshakeValidation.getServerPayload().asNioByteBuffer());
            }

            if (LOG.isDebugEnabled()) {
                debug("%s: %s: Handshake validated successfully", address, side);
            }
        }
    }

    // retransmission if timeout
    private boolean onReceiveTimeout() throws IOException {
        final HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == HandshakeStatus.NOT_HANDSHAKING) {
            return false;
        } else {
            // retransmission of handshake messages
            return produceHandshakePackets("Reproduced");
        }
    }

    // produce handshake packets
    //CHECKSTYLE:OFF
    private boolean produceHandshakePackets(final String action) throws IOException {
        //CHECKSTYLE:ON

        int packets = 0;
        boolean endLoops = false;
        int loops = MAX_PRODUCE_HANDSHAKE_PACKETS_LOOPS;
        while (!endLoops) {

            if (--loops < 0) {
                throw new IOException("Too many loops to produce handshake packets");
            }

            handshakeBuffer.clear();
            final java.nio.ByteBuffer oApp = EmptyByteBuffer.EMPTY_DIRECT_BYTE_BUFFER;
            final java.nio.ByteBuffer oNet = handshakeBuffer;
            final SSLEngineResult r = engine.wrap(oApp, oNet);
            oNet.flip();

            // Status.OK:
            if (oNet.hasRemaining()) {
                packets++;
                if (LOG.isTraceEnabled()) {
                    printHex(address, side, action + " packet", oNet);
                }
                writer.write(ByteBuffers.wrapRelative(oNet));
            }

            final Status rs = r.getStatus();
            final HandshakeStatus hs = r.getHandshakeStatus();
            if (LOG.isDebugEnabled()) {
                debug("%s: %s: ----produce handshake packet(%s, %s, %s)----", address, side, loops, rs, hs);
            }
            if (rs == Status.BUFFER_OVERFLOW) {
                // the client maximum fragment size config does not work?
                throw new IOException("Buffer overflow: incorrect server maximum fragment size");
            } else if (rs == Status.BUFFER_UNDERFLOW) {
                if (LOG.isDebugEnabled()) {
                    debug("%s: %s: Produce handshake packets: BUFFER_UNDERFLOW occured", address, side);
                    debug("%s: %s: Produce handshake packets: Handshake status: %s", address, side, hs);
                }
                // bad packet, or the client maximum fragment size
                // config does not work?
                if (hs != HandshakeStatus.NOT_HANDSHAKING) {
                    throw new IOException("Buffer underflow: incorrect server maximum fragment size");
                } // otherwise, ignore this packet
            } else if (rs == Status.CLOSED) {
                throw FastEOFException.getInstance("SSLEngine has closed");
                //CHECKSTYLE:OFF
            } else if (rs == Status.OK) {
                //CHECKSTYLE:ON
                // OK
            } else {
                throw new IOException("Can't reach here, result is " + rs);
            }

            if (hs == HandshakeStatus.FINISHED) {
                if (LOG.isDebugEnabled()) {
                    debug("%s: %s: Produce handshake packets: Handshake status is FINISHED, finish the loop", address,
                            side);
                    debug("%s: %s: Produced %s packets", address, side, packets);
                }
                return true;
            }

            boolean endInnerLoop = false;
            HandshakeStatus nhs = hs;
            while (!endInnerLoop) {
                if (nhs == HandshakeStatus.NEED_TASK) {
                    runDelegatedTasks(engine);
                } else if (nhs == HandshakeStatus.NEED_UNWRAP || nhs == HandshakeStatus.NEED_UNWRAP_AGAIN
                        || nhs == HandshakeStatus.NOT_HANDSHAKING) {

                    endInnerLoop = true;
                    endLoops = true;
                } else if (nhs == HandshakeStatus.NEED_WRAP) {
                    endInnerLoop = true;
                } else if (nhs == HandshakeStatus.FINISHED) {
                    throw new IOException(
                            "Unexpected status, SSLEngine.getHandshakeStatus() shouldn't return FINISHED");
                } else {
                    throw new IOException("Can't reach here, handshake status is " + nhs);
                }
                nhs = engine.getHandshakeStatus();
            }
        }

        if (LOG.isDebugEnabled()) {
            debug("%s: %s: %s %s packets", address, side, action, packets);
        }
        return false;
    }

    // run delegated tasks
    private void runDelegatedTasks(final SSLEngine engine) throws IOException {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
            runnable.run();
        }

        final HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == HandshakeStatus.NEED_TASK) {
            throw new IOException("handshake shouldn't need additional tasks");
        }
    }

    // deliver application data
    private void deliverAppData(final java.nio.ByteBuffer appData) throws IOException {
        // Note: have not considered the packet loses
        produceApplicationPackets(appData);
        appData.flip();
    }

    // produce application packets
    void produceApplicationPackets(final java.nio.ByteBuffer appData) throws IOException {

        handshakeBuffer.clear();
        final java.nio.ByteBuffer appNet = handshakeBuffer;
        final SSLEngineResult r = engine.wrap(appData, appNet);
        appNet.flip();

        final Status rs = r.getStatus();
        if (rs == Status.BUFFER_OVERFLOW) {
            // the client maximum fragment size config does not work?
            throw new IOException("Buffer overflow: incorrect server maximum fragment size");
        } else if (rs == Status.BUFFER_UNDERFLOW) {
            // unlikely
            throw new IOException("Buffer underflow during wraping");
        } else if (rs == Status.CLOSED) {
            throw FastEOFException.getInstance("SSLEngine has closed");
            //CHECKSTYLE:OFF
        } else if (rs == Status.OK) {
            //CHECKSTYLE:ON
            // OK
        } else {
            throw new IOException("Can't reach here, result is " + rs);
        }

        // Status.OK:
        if (appNet.hasRemaining()) {
            writer.write(ByteBuffers.wrapRelative(appNet));
        }
    }

    // receive application data
    private void receiveAppData(final java.nio.ByteBuffer expectedApp) throws IOException, TimeoutException {

        int loops = MAX_APP_READ_LOOPS;
        while (true) {
            if (--loops < 0) {
                throw new IOException("Too many loops to receive application data");
            }
            try {
                if (!readerSpinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                    throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
                }
            } catch (final Exception e) {
                throw new IOException(e);
            }
            final IByteBuffer message = reader.readMessage().asBuffer();
            handshakeBuffer.clear();
            final java.nio.ByteBuffer netBuffer = message.asNioByteBuffer();
            final java.nio.ByteBuffer recBuffer = handshakeBuffer;
            final SSLEngineResult rs = engine.unwrap(netBuffer, recBuffer);
            reader.readFinished();
            recBuffer.flip();
            if (recBuffer.remaining() != 0) {
                if (LOG.isTraceEnabled()) {
                    printHex(address, side, "Received application data", recBuffer);
                }
                if (!recBuffer.equals(expectedApp)) {
                    if (LOG.isDebugEnabled()) {
                        debug("%s: %s: Engine status is %s", address, side, rs);
                    }
                    throw new IOException("Not the right application data");
                }
                break;
            }
        }
    }

    private void debug(final String message, final Object... args) {
        synchronized (LOG) {
            LOG.debug(message, args);
        }
    }

    private void printHex(final SocketAddress address, final String side, final String prefix,
            final java.nio.ByteBuffer bb) {
        synchronized (LOG) {
            try (PooledFastByteArrayOutputStream bos = PooledFastByteArrayOutputStream.newInstance()) {
                final IByteBuffer wrap = ByteBuffers.wrapRelative(bb);
                HexDump.dump(wrap.asByteArray(0, wrap.capacity()), 0, bos, 0);
                LOG.trace("%s: %s: \n%s", address, prefix, bos.toString());
            } catch (final Exception e) {
                // ignore
            }
        }
    }

}
