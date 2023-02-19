package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import org.apache.commons.io.HexDump;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;

/**
 * An example to show the way to use SSLEngine in datagram connections.
 * 
 * Original from:
 * https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/master/test/jdk/javax/net/ssl/DTLS/DTLSOverDatagram.java
 * 
 * Debug with -Djavax.net.debug=ssl,handshake
 */
@NotThreadSafe
//CHECKSTYLE:OFF
public class DTLSOverDatagramTest {

    private static int MAX_HANDSHAKE_LOOPS = 200;
    private static int MAX_APP_READ_LOOPS = 60;
    private static int SOCKET_TIMEOUT = 10 * 1000; // in millis
    private static int BUFFER_SIZE = SynchronousChannels.MAX_UNFRAGMENTED_DATAGRAM_PACKET_SIZE;
    private static int MAXIMUM_PACKET_SIZE = SynchronousChannels.MAX_UNFRAGMENTED_DATAGRAM_PACKET_SIZE;

    private static Exception clientException = null;
    private static Exception serverException = null;

    private static java.nio.ByteBuffer serverApp = java.nio.ByteBuffer.wrap("Hi Client, I'm Server".getBytes());
    private static java.nio.ByteBuffer clientApp = java.nio.ByteBuffer.wrap("Hi Server, I'm Client".getBytes());

    /*
     * Define the server side of the test.
     */
    void doServerSide(final DatagramSocket socket, final InetSocketAddress serverSocketAddr,
            final InetSocketAddress clientSocketAddr) throws Exception {

        // create SSLEngine
        final SSLEngine engine = createSSLEngine(serverSocketAddr, false);

        // handshaking
        handshake(engine, socket, clientSocketAddr, "Server");

        // read client application data
        receiveAppData(engine, socket, clientApp);

        // write server application data
        deliverAppData(engine, socket, serverApp, clientSocketAddr);
    }

    /*
     * Define the client side of the test.
     */
    void doClientSide(final DatagramSocket socket, final InetSocketAddress serverSocketAddr) throws Exception {

        // create SSLEngine
        final SSLEngine engine = createSSLEngine(serverSocketAddr, true);

        // handshaking
        handshake(engine, socket, serverSocketAddr, "Client");

        // write client application data
        deliverAppData(engine, socket, clientApp, serverSocketAddr);

        // read server application data
        receiveAppData(engine, socket, serverApp);
    }

    /*
     * ============================================================= The remainder is support stuff for DTLS operations.
     */
    SSLEngine createSSLEngine(final InetSocketAddress serverSocketAddr, final boolean isClient) throws Exception {
        final SSLContext context = getDTLSContext(serverSocketAddr, !isClient);
        final SSLEngine engine = context.createSSLEngine();

        final SSLParameters paras = engine.getSSLParameters();
        paras.setMaximumPacketSize(MAXIMUM_PACKET_SIZE);

        engine.setUseClientMode(isClient);
        engine.setSSLParameters(paras);

        return engine;
    }

    // handshake
    void handshake(final SSLEngine engine, final DatagramSocket socket, final SocketAddress peerAddr, final String side)
            throws Exception {

        boolean endLoops = false;
        int loops = MAX_HANDSHAKE_LOOPS;
        engine.beginHandshake();
        while (!endLoops && (serverException == null) && (clientException == null)) {

            if (--loops < 0) {
                throw new RuntimeException("Too many loops to produce handshake packets");
            }

            SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
            log(side, "=======handshake(" + loops + ", " + hs + ")=======");
            if (hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                    || hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN) {

                log(side, "Receive DTLS records, handshake status is " + hs);

                java.nio.ByteBuffer iNet;
                java.nio.ByteBuffer iApp;
                if (hs == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    final byte[] buf = new byte[BUFFER_SIZE];
                    final DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    try {
                        socket.receive(packet);
                    } catch (final SocketTimeoutException ste) {
                        log(side, "Warning: " + ste);

                        final List<DatagramPacket> packets = new ArrayList<>();
                        final boolean finished = onReceiveTimeout(engine, peerAddr, side, packets);

                        log(side, "Reproduced " + packets.size() + " packets");
                        for (final DatagramPacket p : packets) {
                            printHex("Reproduced packet", p.getData(), p.getOffset(), p.getLength());
                            socket.send(p);
                        }

                        if (finished) {
                            log(side, "Handshake status is FINISHED " + "after calling onReceiveTimeout(), "
                                    + "finish the loop");
                            endLoops = true;
                        }

                        log(side, "New handshake status is " + engine.getHandshakeStatus());

                        continue;
                    }

                    iNet = java.nio.ByteBuffer.wrap(buf, 0, packet.getLength());
                    iApp = java.nio.ByteBuffer.allocate(BUFFER_SIZE);
                } else {
                    iNet = java.nio.ByteBuffer.allocate(0);
                    iApp = java.nio.ByteBuffer.allocate(BUFFER_SIZE);
                }

                final SSLEngineResult r = engine.unwrap(iNet, iApp);
                final SSLEngineResult.Status rs = r.getStatus();
                hs = r.getHandshakeStatus();
                if (rs == SSLEngineResult.Status.OK) {
                    // OK
                } else if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                    log(side, "BUFFER_OVERFLOW, handshake status is " + hs);

                    // the client maximum fragment size config does not work?
                    throw new Exception("Buffer overflow: " + "incorrect client maximum fragment size");
                } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    log(side, "BUFFER_UNDERFLOW, handshake status is " + hs);

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
                    log(side, "Handshake status is FINISHED, finish the loop");
                    endLoops = true;
                }
            } else if (hs == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                final List<DatagramPacket> packets = new ArrayList<>();
                final boolean finished = produceHandshakePackets(engine, peerAddr, side, packets);

                log(side, "Produced " + packets.size() + " packets");
                for (final DatagramPacket p : packets) {
                    socket.send(p);
                }

                if (finished) {
                    log(side, "Handshake status is FINISHED " + "after producing handshake packets, "
                            + "finish the loop");
                    endLoops = true;
                }
            } else if (hs == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                runDelegatedTasks(engine);
            } else if (hs == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                log(side, "Handshake status is NOT_HANDSHAKING, finish the loop");
                endLoops = true;
            } else if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
                throw new Exception("Unexpected status, SSLEngine.getHandshakeStatus() " + "shouldn't return FINISHED");
            } else {
                throw new Exception("Can't reach here, handshake status is " + hs);
            }
        }

        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        log(side, "Handshake finished, status is " + hs);

        if (engine.getHandshakeSession() != null) {
            throw new Exception("Handshake finished, but handshake session is not null");
        }

        final SSLSession session = engine.getSession();
        if (session == null) {
            throw new Exception("Handshake finished, but session is null");
        }
        log(side, "Negotiated protocol is " + session.getProtocol());
        log(side, "Negotiated cipher suite is " + session.getCipherSuite());

        // handshake status should be NOT_HANDSHAKING
        //
        // According to the spec, SSLEngine.getHandshakeStatus() can't
        // return FINISHED.
        if (hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new Exception("Unexpected handshake status " + hs);
        }
    }

    // deliver application data
    void deliverAppData(final SSLEngine engine, final DatagramSocket socket, final java.nio.ByteBuffer appData,
            final SocketAddress peerAddr) throws Exception {

        // Note: have not consider the packet loses
        final List<DatagramPacket> packets = produceApplicationPackets(engine, appData, peerAddr);
        ByteBuffers.flip(appData);
        for (final DatagramPacket p : packets) {
            socket.send(p);
        }
    }

    // receive application data
    void receiveAppData(final SSLEngine engine, final DatagramSocket socket, final java.nio.ByteBuffer expectedApp)
            throws Exception {

        int loops = MAX_APP_READ_LOOPS;
        while ((serverException == null) && (clientException == null)) {
            if (--loops < 0) {
                throw new RuntimeException("Too many loops to receive application data");
            }

            final byte[] buf = new byte[BUFFER_SIZE];
            final DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            final java.nio.ByteBuffer netBuffer = java.nio.ByteBuffer.wrap(buf, 0, packet.getLength());
            final java.nio.ByteBuffer recBuffer = java.nio.ByteBuffer.allocate(BUFFER_SIZE);
            final SSLEngineResult rs = engine.unwrap(netBuffer, recBuffer);
            ByteBuffers.flip(recBuffer);
            if (recBuffer.remaining() != 0) {
                printHex("Received application data", recBuffer);
                if (!recBuffer.equals(expectedApp)) {
                    System.out.println("Engine status is " + rs);
                    throw new Exception("Not the right application data");
                }
                break;
            }
        }
    }

    // produce handshake packets
    boolean produceHandshakePackets(final SSLEngine engine, final SocketAddress socketAddr, final String side,
            final List<DatagramPacket> packets) throws Exception {

        boolean endLoops = false;
        int loops = MAX_HANDSHAKE_LOOPS / 2;
        while (!endLoops && (serverException == null) && (clientException == null)) {

            if (--loops < 0) {
                throw new RuntimeException("Too much loops to produce handshake packets");
            }

            final java.nio.ByteBuffer oNet = java.nio.ByteBuffer.allocate(32768);
            final java.nio.ByteBuffer oApp = java.nio.ByteBuffer.allocate(0);
            final SSLEngineResult r = engine.wrap(oApp, oNet);
            ByteBuffers.flip(oNet);

            final SSLEngineResult.Status rs = r.getStatus();
            final SSLEngineResult.HandshakeStatus hs = r.getHandshakeStatus();
            log(side, "----produce handshake packet(" + loops + ", " + rs + ", " + hs + ")----");
            if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                // the client maximum fragment size config does not work?
                throw new Exception("Buffer overflow: " + "incorrect server maximum fragment size");
            } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                log(side, "Produce handshake packets: BUFFER_UNDERFLOW occured");
                log(side, "Produce handshake packets: Handshake status: " + hs);
                // bad packet, or the client maximum fragment size
                // config does not work?
                if (hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                    throw new Exception("Buffer underflow: " + "incorrect server maximum fragment size");
                } // otherwise, ignore this packet
            } else if (rs == SSLEngineResult.Status.CLOSED) {
                throw new Exception("SSLEngine has closed");
            } else if (rs == SSLEngineResult.Status.OK) {
                // OK
            } else {
                throw new Exception("Can't reach here, result is " + rs);
            }

            // SSLEngineResult.Status.OK:
            if (oNet.hasRemaining()) {
                final byte[] ba = new byte[oNet.remaining()];
                oNet.get(ba);
                final DatagramPacket packet = createHandshakePacket(ba, socketAddr);
                packets.add(packet);
            }

            if (hs == SSLEngineResult.HandshakeStatus.FINISHED) {
                log(side, "Produce handshake packets: " + "Handshake status is FINISHED, finish the loop");
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

        return false;
    }

    DatagramPacket createHandshakePacket(final byte[] ba, final SocketAddress socketAddr) {
        return new DatagramPacket(ba, ba.length, socketAddr);
    }

    // produce application packets
    List<DatagramPacket> produceApplicationPackets(final SSLEngine engine, final java.nio.ByteBuffer source,
            final SocketAddress socketAddr) throws Exception {

        final List<DatagramPacket> packets = new ArrayList<>();
        final java.nio.ByteBuffer appNet = java.nio.ByteBuffer.allocate(32768);
        final SSLEngineResult r = engine.wrap(source, appNet);
        ByteBuffers.flip(appNet);

        final SSLEngineResult.Status rs = r.getStatus();
        if (rs == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            // the client maximum fragment size config does not work?
            throw new Exception("Buffer overflow: " + "incorrect server maximum fragment size");
        } else if (rs == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            // unlikely
            throw new Exception("Buffer underflow during wraping");
        } else if (rs == SSLEngineResult.Status.CLOSED) {
            throw new Exception("SSLEngine has closed");
        } else if (rs == SSLEngineResult.Status.OK) {
            // OK
        } else {
            throw new Exception("Can't reach here, result is " + rs);
        }

        // SSLEngineResult.Status.OK:
        if (appNet.hasRemaining()) {
            final byte[] ba = new byte[appNet.remaining()];
            appNet.get(ba);
            final DatagramPacket packet = new DatagramPacket(ba, ba.length, socketAddr);
            packets.add(packet);
        }

        return packets;
    }

    // Get a datagram packet for the specified handshake type.
    static DatagramPacket getPacket(final List<DatagramPacket> packets, final byte handshakeType) {
        boolean matched = false;
        for (final DatagramPacket packet : packets) {
            final byte[] data = packet.getData();
            final int offset = packet.getOffset();
            final int length = packet.getLength();

            // Normally, this pakcet should be a handshake message
            // record.  However, even if the underlying platform
            // splits the record more, we don't really worry about
            // the improper packet loss because DTLS implementation
            // should be able to handle packet loss properly.
            //
            // See RFC 6347 for the detailed format of DTLS records.
            if (handshakeType == -1) { // ChangeCipherSpec
                // Is it a ChangeCipherSpec message?
                matched = (length == 14) && (data[offset] == 0x14);
            } else if ((length >= 25) && // 25: handshake mini size
                    (data[offset] == 0x16)) { // a handshake message

                // check epoch number for initial handshake only
                if (data[offset + 3] == 0x00) { // 3,4: epoch
                    if (data[offset + 4] == 0x00) { // plaintext
                        matched = (data[offset + 13] == handshakeType);
                    } else { // cipherext
                        // The 1st ciphertext is a Finished message.
                        //
                        // If it is not proposed to loss the Finished
                        // message, it is not necessary to check the
                        // following packets any mroe as a Finished
                        // message is the last handshake message.
                        matched = (handshakeType == 20);
                    }
                }
            }

            if (matched) {
                return packet;
            }
        }

        return null;
    }

    // run delegated tasks
    void runDelegatedTasks(final SSLEngine engine) throws Exception {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
            runnable.run();
        }

        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == SSLEngineResult.HandshakeStatus.NEED_TASK) {
            throw new Exception("handshake shouldn't need additional tasks");
        }
    }

    // retransmission if timeout
    boolean onReceiveTimeout(final SSLEngine engine, final SocketAddress socketAddr, final String side,
            final List<DatagramPacket> packets) throws Exception {

        final SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();
        if (hs == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            return false;
        } else {
            // retransmission of handshake messages
            return produceHandshakePackets(engine, socketAddr, side, packets);
        }
    }

    // get DTSL context
    SSLContext getDTLSContext(final InetSocketAddress socketAddress, final boolean server) throws Exception {
        final ITransportLayerSecurityProvider provider = new DerivedKeyTransportLayerSecurityProvider(socketAddress,
                server) {
            @Override
            protected String getHostname() {
                return socketAddress.getHostName();
            }
        };
        return provider.newContext();

        //        final String keyStoreFile = "keystore";
        //        final String trustStoreFile = "truststore";
        //        final String passwd = "passphrase";
        //
        //        final KeyStore ks = KeyStore.getInstance("JKS");
        //        final KeyStore ts = KeyStore.getInstance("JKS");
        //
        //        final char[] passphrase = "passphrase".toCharArray();
        //
        //        try (InputStream fis = new ClassPathResource(keyStoreFile, getClass()).getInputStream()) {
        //            ks.load(fis, passphrase);
        //        }
        //
        //        try (InputStream fis = new ClassPathResource(trustStoreFile, getClass()).getInputStream()) {
        //            ts.load(fis, passphrase);
        //        }
        //
        //        final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        //        kmf.init(ks, passphrase);
        //
        //        final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        //        tmf.init(ts);
        //
        //        final SSLContext sslCtx = SSLContext.getInstance("DTLS");
        //
        //        sslCtx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        //
        //        return sslCtx;
    }

    /*
     * ============================================================= The remainder is support stuff to kickstart the
     * testing.
     */

    // Will the handshaking and application data exchange succeed?
    public boolean isGoodJob() {
        return true;
    }

    @Test
    public void runTest() throws Exception {
        try (DatagramSocket serverSocket = new DatagramSocket(); DatagramSocket clientSocket = new DatagramSocket()) {

            serverSocket.setSoTimeout(SOCKET_TIMEOUT);
            clientSocket.setSoTimeout(SOCKET_TIMEOUT);

            final InetAddress localHost = InetAddress.getLocalHost();
            final InetSocketAddress serverSocketAddr = new InetSocketAddress(localHost, serverSocket.getLocalPort());

            final InetSocketAddress clientSocketAddr = new InetSocketAddress(localHost, clientSocket.getLocalPort());

            final ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(2);
            Future<String> server, client;

            try {
                server = pool.submit(new ServerCallable(this, serverSocket, serverSocketAddr, clientSocketAddr));
                client = pool.submit(new ClientCallable(this, clientSocket, serverSocketAddr));
            } finally {
                pool.shutdown();
            }

            boolean failed = false;

            // wait for client to finish
            try {
                System.out.println("Client finished: " + client.get());
            } catch (CancellationException | InterruptedException | ExecutionException e) {
                System.out.println("Exception on client side: ");
                e.printStackTrace(System.out);
                failed = true;
            }

            // wait for server to finish
            try {
                System.out.println("Client finished: " + server.get());
            } catch (CancellationException | InterruptedException | ExecutionException e) {
                System.out.println("Exception on server side: ");
                e.printStackTrace(System.out);
                failed = true;
            }

            if (failed) {
                throw new RuntimeException("Test failed");
            }
        }
    }

    final static class ServerCallable implements Callable<String> {

        private final DTLSOverDatagramTest testCase;
        private final DatagramSocket socket;
        private final InetSocketAddress serverSocketAddr;
        private final InetSocketAddress clientSocketAddr;

        ServerCallable(final DTLSOverDatagramTest testCase, final DatagramSocket socket,
                final InetSocketAddress serverSocketAddr, final InetSocketAddress clientSocketAddr) {

            this.testCase = testCase;
            this.socket = socket;
            this.serverSocketAddr = serverSocketAddr;
            this.clientSocketAddr = clientSocketAddr;
        }

        @Override
        public String call() throws Exception {
            try {
                testCase.doServerSide(socket, serverSocketAddr, clientSocketAddr);
            } catch (final Exception e) {
                System.out.println("Exception in  ServerCallable.call():");
                e.printStackTrace(System.out);
                serverException = e;

                if (testCase.isGoodJob()) {
                    throw e;
                } else {
                    return "Well done, server!";
                }
            }

            if (testCase.isGoodJob()) {
                return "Well done, server!";
            } else {
                throw new Exception("No expected exception");
            }
        }
    }

    final static class ClientCallable implements Callable<String> {

        private final DTLSOverDatagramTest testCase;
        private final DatagramSocket socket;
        private final InetSocketAddress serverSocketAddr;

        ClientCallable(final DTLSOverDatagramTest testCase, final DatagramSocket socket,
                final InetSocketAddress serverSocketAddr) {

            this.testCase = testCase;
            this.socket = socket;
            this.serverSocketAddr = serverSocketAddr;
        }

        @Override
        public String call() throws Exception {
            try {
                testCase.doClientSide(socket, serverSocketAddr);
            } catch (final Exception e) {
                System.out.println("Exception in ClientCallable.call():");
                e.printStackTrace(System.out);
                clientException = e;

                if (testCase.isGoodJob()) {
                    throw e;
                } else {
                    return "Well done, client!";
                }
            }

            if (testCase.isGoodJob()) {
                return "Well done, client!";
            } else {
                throw new Exception("No expected exception");
            }
        }
    }

    final static void printHex(final String prefix, final java.nio.ByteBuffer bb) {
        synchronized (System.out) {
            System.out.println(prefix);
            try {
                HexDump.dump(ByteBuffers.wrap(bb.slice()).asByteArray(), 0, System.out, 0);
            } catch (final Exception e) {
                // ignore
            }
            System.out.flush();
        }
    }

    final static void printHex(final String prefix, final byte[] bytes, final int offset, final int length) {

        synchronized (System.out) {
            System.out.println(prefix);
            try {
                final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes, offset, length);
                HexDump.dump(ByteBuffers.wrap(bb).asByteArray(), 0, System.out, 0);
            } catch (final Exception e) {
                // ignore
            }
            System.out.flush();
        }
    }

    static void log(final String side, final String message) {
        System.out.println(side + ": " + message);
    }
}