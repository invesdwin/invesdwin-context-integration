// @NotThreadSafe
// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.ucx.jucx.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

public class UcpListenerTest extends UcxTest {

    static Stream<NetworkInterface> getInterfaces() {
        try {
            return Collections.list(NetworkInterface.getNetworkInterfaces()).stream().filter(iface -> {
                try {
                    return iface.isUp() && !iface.isLoopback() && !iface.isVirtual()
                            && !iface.getName().contains("docker");
                } catch (final SocketException e) {
                    return false;
                }
            });
        } catch (final SocketException e) {
            return Stream.empty();
        }
    }

    /**
     * Iterates over network interfaces and tries to bind and create listener on a specific socket address.
     */
    static UcpListener tryBindListener(final UcpWorker worker, final UcpListenerParams params) {
        UcpListener result = null;
        final List<InetAddress> addresses = getInterfaces()
                .flatMap(iface -> Collections.list(iface.getInetAddresses()).stream())
                .filter(addr -> !addr.isLinkLocalAddress())
                .collect(Collectors.toList());
        Collections.reverse(addresses);
        for (final InetAddress address : addresses) {
            result = worker.newListener(params.setSockAddr(new InetSocketAddress(address, 0)));
        }
        Assertions.assertNotNull(result, "Could not find socket address to start UcpListener");
        Assertions.assertNotEquals(0, result.getAddress().getPort());
        System.out.println("Bound UcpListner on: " + result.getAddress());
        return result;
    }

    @Test
    public void testConnectionHandler() throws Exception {
        final long clientId = 3L;
        final UcpContext context1 = new UcpContext(new UcpParams().requestStreamFeature().requestRmaFeature());
        final UcpContext context2 = new UcpContext(new UcpParams().requestStreamFeature().requestRmaFeature());
        final UcpWorker serverWorker1 = context1.newWorker(new UcpWorkerParams());
        final UcpWorker serverWorker2 = context1.newWorker(new UcpWorkerParams());
        final UcpWorker clientWorker = context2.newWorker(new UcpWorkerParams().setClientId(clientId));

        final AtomicReference<UcpConnectionRequest> connRequest = new AtomicReference<>(null);
        final AtomicReference<UcpConnectionRequest> connReject = new AtomicReference<>(null);

        // Create listener and set connection handler
        final UcpListenerParams listenerParams = new UcpListenerParams()
                .setConnectionHandler((final UcpConnectionRequest connectionRequest) -> {
                    if (connRequest.get() == null) {
                        connRequest.set(connectionRequest);
                    } else {
                        connReject.set(connectionRequest);
                    }
                });
        final UcpListener serverListener = tryBindListener(serverWorker1, listenerParams);
        final UcpListener clientListener = tryBindListener(clientWorker, listenerParams);

        final UcpEndpoint clientToServer = clientWorker.newEndpoint(new UcpEndpointParams().sendClientId()
                .setErrorHandler((ep, status, errorMsg) -> System.err.println("clientToServer error: " + errorMsg))
                .setPeerErrorHandlingMode()
                .setSocketAddress(serverListener.getAddress()));

        while (connRequest.get() == null) {
            serverWorker1.progress();
            clientWorker.progress();
        }

        Assertions.assertEquals(clientId, connRequest.get().getClientId());
        Assertions.assertNotNull(connRequest.get().getClientAddress());
        final UcpEndpoint serverToClientListener = serverWorker2.newEndpoint(new UcpEndpointParams()
                .setSocketAddress(connRequest.get().getClientAddress())
                .setPeerErrorHandlingMode()
                .setErrorHandler(
                        (errEp, status, errorMsg) -> System.err.println("serverToClientListener error: " + errorMsg)));
        serverWorker2.progressRequest(serverToClientListener.closeNonBlockingForce());

        // Create endpoint from another worker from pool.
        final UcpEndpoint serverToClient = serverWorker2
                .newEndpoint(new UcpEndpointParams().setConnectionRequest(connRequest.get()));

        // Test connection handler persists
        for (int i = 0; i < 10; i++) {
            clientWorker.newEndpoint(new UcpEndpointParams().setSocketAddress(serverListener.getAddress())
                    .setPeerErrorHandlingMode()
                    .setErrorHandler((ep, status, errorMsg) -> {
                        ep.close();
                        Assertions.assertEquals(UcsConstants.STATUS.UCS_ERR_REJECTED, status);
                    }));

            while (connReject.get() == null) {
                serverWorker1.progress();
                serverWorker2.progress();
                clientWorker.progress();
            }

            connReject.get().reject();
            connReject.set(null);

        }

        final UcpRequest sent = serverToClient.sendStreamNonBlocking(ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE),
                null);

        // Progress all workers to make sure recv request will complete immediately
        for (int i = 0; i < 10; i++) {
            serverWorker1.progress();
            serverWorker2.progress();
            clientWorker.progress();
            try {
                Thread.sleep(2);
            } catch (final Exception ignored) {
            }
        }

        final UcpRequest recv = clientToServer.recvStreamNonBlocking(ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE),
                0, null);

        while (!sent.isCompleted() || !recv.isCompleted()) {
            serverWorker1.progress();
            serverWorker2.progress();
            clientWorker.progress();
        }

        Assertions.assertEquals(UcpMemoryTest.MEM_SIZE, recv.getRecvSize());

        final UcpRequest serverClose = serverToClient.closeNonBlockingFlush();
        final UcpRequest clientClose = clientToServer.closeNonBlockingFlush();

        while (!serverClose.isCompleted() || !clientClose.isCompleted()) {
            serverWorker2.progress();
            clientWorker.progress();
        }

        Collections.addAll(resources, context2, context1, clientWorker, serverWorker1, serverWorker2, serverListener,
                clientListener);
        closeResources();
    }
}
