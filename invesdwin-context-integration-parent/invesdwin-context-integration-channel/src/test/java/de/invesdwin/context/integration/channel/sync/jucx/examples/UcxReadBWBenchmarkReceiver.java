package de.invesdwin.context.integration.channel.sync.jucx.examples;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;

public class UcxReadBWBenchmarkReceiver extends UcxBenchmark {

    public static void main(final String[] args) throws Exception {
        if (!initializeArguments(args)) {
            return;
        }

        createContextAndWorker();

        final String serverHost = argsMap.get("s");
        final InetSocketAddress sockaddr = new InetSocketAddress(serverHost, serverPort);
        final AtomicReference<UcpConnectionRequest> connRequest = new AtomicReference<>(null);
        final UcpListener listener = worker
                .newListener(new UcpListenerParams().setConnectionHandler(connRequest::set).setSockAddr(sockaddr));
        resources.push(listener);
        System.out.println("Waiting for connections on " + sockaddr + " ...");

        while (connRequest.get() == null) {
            worker.progress();
        }

        final UcpEndpoint endpoint = worker.newEndpoint(
                new UcpEndpointParams().setConnectionRequest(connRequest.get()).setPeerErrorHandlingMode());

        final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(4096);
        final UcpRequest recvRequest = worker.recvTaggedNonBlocking(recvBuffer, null);

        worker.progressRequest(recvRequest);

        final long remoteAddress = recvBuffer.getLong();
        final long remoteSize = recvBuffer.getLong();
        final int remoteKeySize = recvBuffer.getInt();
        final int rkeyBufferOffset = recvBuffer.position();

        recvBuffer.position(rkeyBufferOffset + remoteKeySize);
        final int remoteHashCode = recvBuffer.getInt();
        System.out.printf("Received connection. Will read %d bytes from remote address %d%n", remoteSize,
                remoteAddress);

        recvBuffer.position(rkeyBufferOffset);
        final UcpRemoteKey remoteKey = endpoint.unpackRemoteKey(recvBuffer);
        resources.push(remoteKey);

        final UcpMemory recvMemory = context.memoryMap(allocationParams);
        resources.push(recvMemory);
        final ByteBuffer data = UcxUtils.getByteBufferView(recvMemory.getAddress(),
                Math.min(Integer.MAX_VALUE, totalSize));
        for (int i = 0; i < numIterations; i++) {
            final int iterNum = i;
            final UcpRequest getRequest = endpoint.getNonBlocking(remoteAddress, remoteKey, recvMemory.getAddress(),
                    remoteSize, new UcxCallback() {
                        final long startTime = System.nanoTime();

                        @Override
                        public void onSuccess(final UcpRequest request) {
                            final long finishTime = System.nanoTime();
                            data.clear();
                            assert data.hashCode() == remoteHashCode;
                            final double bw = getBandwithGbits(finishTime - startTime, remoteSize);
                            System.out.printf("Iteration %d, bandwidth: %.4f GB/s%n", iterNum, bw);
                        }
                    });

            worker.progressRequest(getRequest);
            // To make sure we receive correct data each time to compare hashCodes
            data.put(0, (byte) 1);
        }

        final UcpRequest closeRequest = endpoint.closeNonBlockingFlush();
        worker.progressRequest(closeRequest);

        closeResources();
    }
}
