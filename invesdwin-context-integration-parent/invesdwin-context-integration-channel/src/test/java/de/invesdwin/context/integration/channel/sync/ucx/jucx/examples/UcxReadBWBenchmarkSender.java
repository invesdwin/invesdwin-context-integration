package de.invesdwin.context.integration.channel.sync.ucx.jucx.examples;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucs.UcsConstants;

public class UcxReadBWBenchmarkSender extends UcxBenchmark {

    public static void main(final String[] args) throws Exception {
        if (!initializeArguments(args)) {
            return;
        }

        createContextAndWorker();

        final String serverHost = argsMap.get("s");
        final UcpEndpoint endpoint = worker.newEndpoint(
                new UcpEndpointParams().setPeerErrorHandlingMode().setErrorHandler((ep, status, errorMsg) -> {
                    if (status == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                        throw new ConnectException(errorMsg);
                    } else {
                        throw new UcxException(errorMsg);
                    }
                }).setSocketAddress(new InetSocketAddress(serverHost, serverPort)));

        final UcpMemory memory = context.memoryMap(allocationParams);
        resources.push(memory);
        final ByteBuffer data = UcxUtils.getByteBufferView(memory.getAddress(), Math.min(Integer.MAX_VALUE, totalSize));

        // Send worker and memory address and Rkey to receiver.
        final ByteBuffer rkeyBuffer = memory.getRemoteKeyBuffer();

        // 24b = 8b buffer address + 8b buffer size + 4b rkeyBuffer size + 4b hashCode
        final ByteBuffer sendData = ByteBuffer.allocateDirect(24 + rkeyBuffer.capacity());
        sendData.putLong(memory.getAddress());
        sendData.putLong(totalSize);
        sendData.putInt(rkeyBuffer.capacity());
        sendData.put(rkeyBuffer);
        sendData.putInt(data.hashCode());
        sendData.clear();

        // Send memory metadata and wait until receiver will finish benchmark.
        endpoint.sendTaggedNonBlocking(sendData, null);

        try {
            while (true) {
                if (worker.progress() == 0) {
                    worker.waitForEvents();
                }
            }
        } catch (final ConnectException ignored) {
        } catch (final Exception ex) {
            System.err.println(ex.getMessage());
        }

        try {
            worker.progressRequest(endpoint.closeNonBlockingForce());
        } catch (final Exception ignored) {
        } finally {
            closeResources();
        }
    }
}
