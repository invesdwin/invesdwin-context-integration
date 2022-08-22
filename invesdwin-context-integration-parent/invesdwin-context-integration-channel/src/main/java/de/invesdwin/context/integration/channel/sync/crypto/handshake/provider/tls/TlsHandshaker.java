package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.net.SocketAddress;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

import org.apache.commons.io.HexDump;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.stream.ByteBufferOutputStream;
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
    private static final int MAX_PRODUCT_HANDSHAKE_PACKETS_LOOPS = MAX_HANDSHAKE_LOOPS / 2;
    private static final int MAX_APP_READ_LOOPS = 60;

    private static final java.nio.ByteBuffer SERVER_APP = java.nio.ByteBuffer.wrap("Hi Client, I'm Server".getBytes());
    private static final java.nio.ByteBuffer CLIENT_APP = java.nio.ByteBuffer.wrap("Hi Server, I'm Client".getBytes());

    private final java.nio.ByteBuffer handshakeBuffer;

    public TlsHandshaker() {
        this.handshakeBuffer = java.nio.ByteBuffer.allocateDirect(HANDSHAKE_BUFFER_CAPACITY);
    }

    //CHECKSTYLE:OFF
    public void performHandshake(final Duration handshakeTimeout, final SocketAddress address,
            final ITlsProtocol protocol, final SSLEngine engine, final ASpinWait readerSpinWait,
            final ISynchronousReader<IByteBuffer> reader, final ISynchronousWriter<IByteBufferWriter> writer)
            throws Exception {
        //CHECKSTYLE:ON
        final boolean client = engine.getUseClientMode();
        final String side = client ? "Client" : "Server";

        boolean endLoops = false;
        int loops = MAX_HANDSHAKE_LOOPS;
        engine.beginHandshake();
        while (!endLoops) {

            if (--loops < 0) {
                throw new RuntimeException("Too many loops to produce handshake packets");
            }

            SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
            LOG.debug("%s: %s: =======handshake(%s, %s)=======", address, side, loops, hs);
            if (hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                    || hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN) {

                LOG.debug("%s: %s: Receive %s records, handshake status is %s", address, side, protocol.name(), hs);

                final boolean readFinishedRequired;
                final java.nio.ByteBuffer iNet;
                final java.nio.ByteBuffer iApp;
                if (hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    if (!readerSpinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                        if (protocol.isHandshakeTimeoutRecoveryEnabled()) {
                            LOG.debug(
                                    "%s: %s: Warning: Trying to recover from read handshake message timeout exceeded: %s",
                                    address, side, handshakeTimeout);

                            final boolean finished = onReceiveTimeout(address, side, engine, writer);
                            //CHECKSTYLE:OFF
                            if (finished) {
                                //CHECKSTYLE:ON
                                LOG.debug(
                                        "%s: %s: Handshake status is FINISHED after calling onReceiveTimeout(), finish the loop",
                                        address, side);
                                endLoops = true;
                            }

                            LOG.debug("%s: %s: New handshake status is %s", address, side, engine.getHandshakeStatus());

                            continue;
                        } else {
                            throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
                        }
                    }
                    final IByteBuffer message = reader.readMessage();
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
                final SSLEngineResult.Status rs = r.getStatus();
                hs = r.getHandshakeStatus();
                //CHECKSTYLE:OFF
                if (rs == SSLEngineResult.Status.OK) {
                    //CHECKSTYLE:ON
                    // OK
                } else if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                    LOG.debug("%s: %s: BUFFER_OVERFLOW, handshake status is %s", address, side, hs);

                    // the client maximum fragment size config does not work?
                    throw new Exception("Buffer overflow: " + "incorrect client maximum fragment size");
                } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    LOG.debug("%s: %s: BUFFER_UNDERFLOW, handshake status is %s", address, side, hs);

                    // bad packet, or the client maximum fragment size
                    // config does not work?
                    if (hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                        throw new Exception("Buffer underflow: " + "incorrect client maximum fragment size");
                    } // otherwise, ignore this packet
                } else if (rs == SSLEngineResult.Status.CLOSED) {
                    throw new Exception("SSL engine closed, handshake status is " + hs);
                } else {
                    throw new Exception("Can't reach here, result is " + rs);
                }

                if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
                    LOG.debug("%s: %s: Handshake status is FINISHED, finish the loop", address, side);
                    endLoops = true;
                }
            } else if (hs == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                final boolean finished = produceHandshakePackets(address, side, "Produced", engine, writer);
                if (finished) {
                    LOG.debug("%s: %s: Handshake status is FINISHED after producing handshake packets, finish the loop",
                            address, side);
                    endLoops = true;
                }
            } else if (hs == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                runDelegatedTasks(engine);
            } else if (hs == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                LOG.debug("%s: %s: Handshake status is NOT_HANDSHAKING, finish the loop", address, side);
                endLoops = true;
            } else if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
                throw new Exception("Unexpected status, SSLEngine.getHandshakeStatus() shouldn't return FINISHED");
            } else {
                throw new Exception("Can't reach here, handshake status is " + hs);
            }
        }

        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        LOG.debug("%s: %s: Handshake finished, status is %s", address, side, hs);

        if (engine.getHandshakeSession() != null) {
            throw new Exception("Handshake finished, but handshake session is not null");
        }

        final SSLSession session = engine.getSession();
        if (session == null) {
            throw new Exception("Handshake finished, but session is null");
        }
        LOG.debug("%s: %s: Negotiated protocol is %s", address, side, session.getProtocol());
        LOG.debug("%s: %s: Negotiated cipher suite is %s", address, side, session.getCipherSuite());

        // handshake status should be NOT_HANDSHAKING
        //
        // According to the spec, SSLEngine.getHandshakeStatus() can't
        // return FINISHED.
        if (hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new Exception("Unexpected handshake status " + hs);
        }

        if (isValidateHandshake()) {
            if (client) {
                // write client application data
                deliverAppData(engine, writer, CLIENT_APP);
                // read server application data
                receiveAppData(address, side, handshakeTimeout, engine, readerSpinWait, reader, SERVER_APP);
            } else {
                // read client application data
                receiveAppData(address, side, handshakeTimeout, engine, readerSpinWait, reader, CLIENT_APP);
                // write server application data
                deliverAppData(engine, writer, SERVER_APP);
            }

            LOG.debug("%s: %s: Handshake validated successfully", address, side);
        }
    }

    protected boolean isValidateHandshake() {
        return true;
    }

    // retransmission if timeout
    private boolean onReceiveTimeout(final SocketAddress address, final String side, final SSLEngine engine,
            final ISynchronousWriter<IByteBufferWriter> writer) throws Exception {
        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            return false;
        } else {
            // retransmission of handshake messages
            return produceHandshakePackets(address, side, "Reproduced", engine, writer);
        }
    }

    // produce handshake packets
    //CHECKSTYLE:OFF
    private boolean produceHandshakePackets(final SocketAddress address, final String side, final String action,
            final SSLEngine engine, final ISynchronousWriter<IByteBufferWriter> writer) throws Exception {
        //CHECKSTYLE:ON

        int packets = 0;
        boolean endLoops = false;
        int loops = MAX_PRODUCT_HANDSHAKE_PACKETS_LOOPS;
        while (!endLoops) {

            if (--loops < 0) {
                throw new RuntimeException("Too much loops to produce handshake packets");
            }

            handshakeBuffer.clear();
            final java.nio.ByteBuffer oApp = EmptyByteBuffer.EMPTY_DIRECT_BYTE_BUFFER;
            final java.nio.ByteBuffer oNet = handshakeBuffer;
            final SSLEngineResult r = engine.wrap(oApp, oNet);
            oNet.flip();

            final SSLEngineResult.Status rs = r.getStatus();
            final SSLEngineResult.HandshakeStatus hs = r.getHandshakeStatus();
            LOG.debug("%s: %s: ----produce handshake packet(%s, %s, %s)----", address, side, loops, rs, hs);
            if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                // the client maximum fragment size config does not work?
                throw new Exception("Buffer overflow: " + "incorrect server maximum fragment size");
            } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                LOG.debug("%s: %s: Produce handshake packets: BUFFER_UNDERFLOW occured", address, side);
                LOG.debug("%s: %s: Produce handshake packets: Handshake status: %s", address, side, hs);
                // bad packet, or the client maximum fragment size
                // config does not work?
                if (hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                    throw new Exception("Buffer underflow: " + "incorrect server maximum fragment size");
                } // otherwise, ignore this packet
            } else if (rs == SSLEngineResult.Status.CLOSED) {
                throw new Exception("SSLEngine has closed");
                //CHECKSTYLE:OFF
            } else if (rs == SSLEngineResult.Status.OK) {
                //CHECKSTYLE:ON
                // OK
            } else {
                throw new Exception("Can't reach here, result is " + rs);
            }

            // SSLEngineResult.Status.OK:
            if (oNet.hasRemaining()) {
                packets++;
                if (LOG.isTraceEnabled()) {
                    printHex(address, side, action + " packet", oNet);
                }
                writer.write(ByteBuffers.wrapRelative(oNet));
            }

            if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
                LOG.debug("%s: %s: Produce handshake packets: Handshake status is FINISHED, finish the loop", address,
                        side);
                LOG.debug("%s: %s: Produced %s packets", address, side, packets);
                return true;
            }

            boolean endInnerLoop = false;
            SSLEngineResult.HandshakeStatus nhs = hs;
            while (!endInnerLoop) {
                if (nhs == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                    runDelegatedTasks(engine);
                } else if (nhs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                        || nhs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN
                        || nhs == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

                    endInnerLoop = true;
                    endLoops = true;
                } else if (nhs == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    endInnerLoop = true;
                } else if (nhs == SSLEngineResult.HandshakeStatus.FINISHED) {
                    throw new Exception(
                            "Unexpected status, SSLEngine.getHandshakeStatus() " + "shouldn't return FINISHED");
                } else {
                    throw new Exception("Can't reach here, handshake status is " + nhs);
                }
                nhs = engine.getHandshakeStatus();
            }
        }

        LOG.debug("%s: %s: %s %s packets", address, side, action, packets);
        return false;
    }

    // run delegated tasks
    private void runDelegatedTasks(final SSLEngine engine) throws Exception {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
            runnable.run();
        }

        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == SSLEngineResult.HandshakeStatus.NEED_TASK) {
            throw new Exception("handshake shouldn't need additional tasks");
        }
    }

    // deliver application data
    private void deliverAppData(final SSLEngine engine, final ISynchronousWriter<IByteBufferWriter> writer,
            final java.nio.ByteBuffer appData) throws Exception {
        // Note: have not considered the packet loses
        produceApplicationPackets(engine, writer, appData);
        appData.flip();
    }

    // produce application packets
    void produceApplicationPackets(final SSLEngine engine, final ISynchronousWriter<IByteBufferWriter> writer,
            final java.nio.ByteBuffer appData) throws Exception {

        handshakeBuffer.clear();
        final java.nio.ByteBuffer appNet = handshakeBuffer;
        final SSLEngineResult r = engine.wrap(appData, appNet);
        appNet.flip();

        final SSLEngineResult.Status rs = r.getStatus();
        if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            // the client maximum fragment size config does not work?
            throw new Exception("Buffer overflow: " + "incorrect server maximum fragment size");
        } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            // unlikely
            throw new Exception("Buffer underflow during wraping");
        } else if (rs == SSLEngineResult.Status.CLOSED) {
            throw new Exception("SSLEngine has closed");
            //CHECKSTYLE:OFF
        } else if (rs == SSLEngineResult.Status.OK) {
            //CHECKSTYLE:ON
            // OK
        } else {
            throw new Exception("Can't reach here, result is " + rs);
        }

        // SSLEngineResult.Status.OK:
        if (appNet.hasRemaining()) {
            writer.write(ByteBuffers.wrapRelative(appNet));
        }
    }

    // receive application data
    private void receiveAppData(final SocketAddress address, final String side, final Duration handshakeTimeout,
            final SSLEngine engine, final ASpinWait readerSpinWait, final ISynchronousReader<IByteBuffer> reader,
            final java.nio.ByteBuffer expectedApp) throws Exception {

        int loops = MAX_APP_READ_LOOPS;
        while (true) {
            if (--loops < 0) {
                throw new RuntimeException("Too many loops to receive application data");
            }
            if (!readerSpinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
            }
            final IByteBuffer message = reader.readMessage();
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
                    LOG.debug("%s: %s: Engine status is %s", address, side, rs);
                    throw new Exception("Not the right application data");
                }
                break;
            }
        }
    }

    private void printHex(final SocketAddress address, final String side, final String prefix,
            final java.nio.ByteBuffer bb) {
        final ByteBufferOutputStream bos = new ByteBufferOutputStream();
        try {
            final IByteBuffer wrap = ByteBuffers.wrapRelative(bb);
            HexDump.dump(wrap.asByteArray(0, wrap.capacity()), 0, bos, 0);
        } catch (final Exception e) {
            // ignore
        }
        LOG.trace("%s: %s: \n%s", address, prefix, bos.toString());
    }

}
