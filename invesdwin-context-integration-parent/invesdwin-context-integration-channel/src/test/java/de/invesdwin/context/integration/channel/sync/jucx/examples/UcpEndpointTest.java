// @NotThreadSafe
// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.jucx.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpAmData;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpRequestParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

public class UcpEndpointTest extends UcxTest {

    public static ArrayList<Integer> memTypes() {
        final ArrayList<Integer> resut = new ArrayList<>();
        resut.add(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
        final UcpContext testContext = new UcpContext(new UcpParams().requestTagFeature());
        final long memTypeMask = testContext.getMemoryTypesMask();
        if (UcsConstants.MEMORY_TYPE.isMemTypeSupported(memTypeMask, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA)) {
            resut.add(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA);
        }
        if (UcsConstants.MEMORY_TYPE.isMemTypeSupported(memTypeMask,
                UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA_MANAGED)) {
            resut.add(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA_MANAGED);
        }
        return resut;
    }

    @Test
    public void testConnectToListenerByWorkerAddr() {
        final UcpContext context = new UcpContext(new UcpParams().requestStreamFeature());
        final UcpWorker worker = context.newWorker(new UcpWorkerParams());
        final UcpEndpointParams epParams = new UcpEndpointParams().setUcpAddress(worker.getAddress())
                .setPeerErrorHandlingMode()
                .setNoLoopbackMode()
                .setName("testConnectToListenerByWorkerAddr");
        final UcpEndpoint endpoint = worker.newEndpoint(epParams);
        assertNotNull(endpoint.getNativeId());

        Collections.addAll(resources, context, worker, endpoint);
        closeResources();
    }

    @Test
    public void testGetNB() throws Exception {
        for (final int memType : memTypes()) {
            System.out.println("Running testGetNB with memType: " + memType);
            // Create 2 contexts + 2 workers
            final UcpParams params = new UcpParams().requestRmaFeature().requestTagFeature();
            final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
            final UcpContext context1 = new UcpContext(params);
            final UcpContext context2 = new UcpContext(params);
            final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
            final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

            // Create endpoint worker1 -> worker2
            final UcpEndpointParams epParams = new UcpEndpointParams().setPeerErrorHandlingMode()
                    .setName("testGetNB")
                    .setUcpAddress(worker2.getAddress());
            final UcpEndpoint endpoint = worker1.newEndpoint(epParams);

            // Allocate 2 source and 2 destination buffers, to perform 2 RDMA Read operations
            final MemoryBlock src1 = allocateMemory(context2, worker2, memType, UcpMemoryTest.MEM_SIZE);
            final MemoryBlock src2 = allocateMemory(context2, worker2, memType, UcpMemoryTest.MEM_SIZE);
            final MemoryBlock dst1 = allocateMemory(context1, worker1, memType, UcpMemoryTest.MEM_SIZE);
            final MemoryBlock dst2 = allocateMemory(context1, worker1, memType, UcpMemoryTest.MEM_SIZE);

            src1.setData(UcpMemoryTest.RANDOM_TEXT);
            src2.setData(UcpMemoryTest.RANDOM_TEXT + UcpMemoryTest.RANDOM_TEXT);

            // Register source buffers on context2
            final UcpMemory remote_memory1 = src1.getMemory();
            final UcpMemory remote_memory2 = src2.getMemory();

            final UcpRemoteKey rkey1 = endpoint.unpackRemoteKey(remote_memory1.getRemoteKeyBuffer());
            final UcpRemoteKey rkey2 = endpoint.unpackRemoteKey(remote_memory2.getRemoteKeyBuffer());

            final AtomicInteger numCompletedRequests = new AtomicInteger(0);

            final UcxCallback callback = new UcxCallback() {
                @Override
                public void onSuccess(final UcpRequest request) {
                    numCompletedRequests.incrementAndGet();
                }
            };

            // Register destination buffers on context1
            final UcpMemory local_memory1 = dst1.getMemory();
            final UcpMemory local_memory2 = dst2.getMemory();

            // Submit 2 get requests
            final UcpRequest request1 = endpoint.getNonBlocking(remote_memory1.getAddress(), rkey1,
                    local_memory1.getAddress(), local_memory1.getLength(), callback,
                    new UcpRequestParams().setMemoryHandle(local_memory1).setMemoryType(memType));
            final UcpRequest request2 = endpoint.getNonBlocking(remote_memory2.getAddress(), rkey2,
                    local_memory2.getAddress(), local_memory2.getLength(), callback,
                    new UcpRequestParams().setMemoryHandle(local_memory2).setMemoryType(memType));

            // Wait for 2 get operations to complete
            while (numCompletedRequests.get() != 2) {
                worker1.progress();
                worker2.progress();
            }

            assertEquals(src1.getData().asCharBuffer(), dst1.getData().asCharBuffer());
            assertEquals(src2.getData().asCharBuffer(), dst2.getData().asCharBuffer());
            assertTrue(request1.isCompleted() && request2.isCompleted());

            Collections.addAll(resources, context2, context1, worker2, worker1, endpoint, rkey2, rkey1, src1, src2,
                    dst1, dst2);
            closeResources();
        }
    }

    @Test
    public void testPutNB() throws Exception {
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestRmaFeature();
        final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);
        final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
        final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

        final ByteBuffer src = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer dst = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        src.asCharBuffer().put(UcpMemoryTest.RANDOM_TEXT);

        // Register destination buffer on context2
        final UcpMemory memory = context2.registerMemory(dst);
        final UcpEndpoint ep = worker1.newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()));

        final UcpRemoteKey rkey = ep.unpackRemoteKey(memory.getRemoteKeyBuffer());
        ep.putNonBlocking(src, memory.getAddress(), rkey, null);

        worker1.progressRequest(worker1.flushNonBlocking(null));

        assertEquals(UcpMemoryTest.RANDOM_TEXT, dst.asCharBuffer().toString().trim());

        Collections.addAll(resources, context2, context1, worker2, worker1, rkey, ep, memory);
        closeResources();
    }

    @Test
    public void testSendRecv() throws Exception {
        for (final int memType : memTypes()) {
            System.out.println("Running testSendRecv with memType: " + memType);
            final long tagSender = 0xFFFFFFFFFFFF0000L;
            // Create 2 contexts + 2 workers
            final UcpParams params = new UcpParams().requestRmaFeature().requestTagFeature();
            final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
            final UcpContext context1 = new UcpContext(params.setTagSenderMask(tagSender));
            final UcpContext context2 = new UcpContext(params);
            final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
            final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

            final MemoryBlock src1 = allocateMemory(context1, worker1, memType, UcpMemoryTest.MEM_SIZE);
            final MemoryBlock src2 = allocateMemory(context1, worker1, memType, UcpMemoryTest.MEM_SIZE);

            final MemoryBlock dst1 = allocateMemory(context2, worker2, memType, UcpMemoryTest.MEM_SIZE);
            final MemoryBlock dst2 = allocateMemory(context2, worker2, memType, UcpMemoryTest.MEM_SIZE);

            src1.setData(UcpMemoryTest.RANDOM_TEXT);
            src2.setData(UcpMemoryTest.RANDOM_TEXT + UcpMemoryTest.RANDOM_TEXT);

            final AtomicInteger receivedMessages = new AtomicInteger(0);
            worker2.recvTaggedNonBlocking(dst1.getMemory().getAddress(), UcpMemoryTest.MEM_SIZE, 0, 0,
                    new UcxCallback() {
                        @Override
                        public void onSuccess(final UcpRequest request) {
                            receivedMessages.incrementAndGet();
                        }
                    }, new UcpRequestParams().setMemoryType(memType).setMemoryHandle(dst1.getMemory()));

            worker2.recvTaggedNonBlocking(dst2.getMemory().getAddress(), UcpMemoryTest.MEM_SIZE, 1, tagSender,
                    new UcxCallback() {
                        @Override
                        public void onSuccess(final UcpRequest request) {
                            receivedMessages.incrementAndGet();
                        }
                    }, new UcpRequestParams().setMemoryType(memType).setMemoryHandle(dst2.getMemory()));

            final UcpEndpoint ep = worker1
                    .newEndpoint(new UcpEndpointParams().setName("testSendRecv").setUcpAddress(worker2.getAddress()));

            ep.sendTaggedNonBlocking(src1.getMemory().getAddress(), UcpMemoryTest.MEM_SIZE, 0, null,
                    new UcpRequestParams().setMemoryType(memType).setMemoryHandle(src1.getMemory()));
            ep.sendTaggedNonBlocking(src2.getMemory().getAddress(), UcpMemoryTest.MEM_SIZE, 1, null,
                    new UcpRequestParams().setMemoryType(memType).setMemoryHandle(src2.getMemory()));

            while (receivedMessages.get() != 2) {
                worker1.progress();
                worker2.progress();
            }

            assertEquals(src1.getData().asCharBuffer(), dst1.getData().asCharBuffer());
            assertEquals(src2.getData().asCharBuffer(), dst2.getData().asCharBuffer());

            Collections.addAll(resources, context2, context1, worker2, worker1, ep, src1, src2, dst1, dst2);
            closeResources();
        }
    }

    @Test
    public void testRecvAfterSend() {
        final long sendTag = 4L;
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestRmaFeature().requestTagFeature().setMtWorkersShared(true);
        final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA().requestThreadSafety();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);
        final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
        final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

        final UcpEndpoint ep = worker1.newEndpoint(new UcpEndpointParams().setPeerErrorHandlingMode()
                .setName("testRecvAfterSend")
                .setErrorHandler((errEp, status, errorMsg) -> {
                })
                .setUcpAddress(worker2.getAddress()));

        final ByteBuffer src1 = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer dst1 = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);

        ep.sendTaggedNonBlocking(src1, sendTag, null);

        final Thread progressThread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        worker1.progress();
                        worker2.progress();
                    } catch (final Exception ex) {
                        System.err.println(ex.getMessage());
                        ex.printStackTrace();
                    }
                }
            }
        };

        progressThread.setDaemon(true);
        progressThread.start();

        try {
            Thread.sleep(5);
        } catch (final InterruptedException ignored) {
        }

        final UcpRequest recv = worker2.recvTaggedNonBlocking(dst1, 0, 0, new UcxCallback() {
            @Override
            public void onSuccess(final UcpRequest request) {
                assertEquals(UcpMemoryTest.MEM_SIZE, request.getRecvSize());
            }
        });

        try {
            int count = 0;
            while ((++count < 100) && !recv.isCompleted()) {
                Thread.sleep(50);
            }
        } catch (final InterruptedException ignored) {
        }

        assertTrue(recv.isCompleted());
        assertEquals(sendTag, recv.getSenderTag());
        final UcpRequest closeRequest = ep.closeNonBlockingForce();

        while (!closeRequest.isCompleted()) {
            try {
                // Wait until progress thread will close the endpoint.
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        progressThread.interrupt();
        try {
            progressThread.join();
        } catch (final InterruptedException ignored) {
        }

        Collections.addAll(resources, context1, context2, worker1, worker2);
        closeResources();
    }

    @Test
    public void testBufferOffset() throws Exception {
        final int msgSize = 200;
        final int offset = 100;
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestTagFeature();
        final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);
        final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
        final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

        final ByteBuffer bigRecvBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer bigSendBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);

        bigRecvBuffer.position(offset).limit(offset + msgSize);
        final UcpRequest recv = worker1.recvTaggedNonBlocking(bigRecvBuffer, 0, 0, null);

        final UcpEndpoint ep = worker2.newEndpoint(new UcpEndpointParams().setUcpAddress(worker1.getAddress()));

        final byte[] msg = new byte[msgSize];
        for (int i = 0; i < msgSize; i++) {
            msg[i] = (byte) i;
        }

        bigSendBuffer.position(offset).limit(offset + msgSize);
        bigSendBuffer.put(msg);
        bigSendBuffer.position(offset);

        final UcpRequest sent = ep.sendTaggedNonBlocking(bigSendBuffer, 0, null);

        while (!sent.isCompleted() || !recv.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        bigSendBuffer.position(offset).limit(offset + msgSize);
        bigRecvBuffer.position(offset).limit(offset + msgSize);
        final ByteBuffer sendData = bigSendBuffer.slice();
        final ByteBuffer recvData = bigRecvBuffer.slice();
        Assertions.assertEquals(sendData, recvData, "Send buffer not equals to recv buffer");

        Collections.addAll(resources, context2, context1, worker2, worker1, ep);
        closeResources();
    }

    @Test
    public void testFlushEp() throws Exception {
        final int numRequests = 10;
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestRmaFeature();
        final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);

        final ByteBuffer src = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        src.asCharBuffer().put(UcpMemoryTest.RANDOM_TEXT);
        final ByteBuffer dst = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final UcpMemory memory = context2.registerMemory(src);

        final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
        final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

        final UcpEndpoint ep = worker1
                .newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()).setPeerErrorHandlingMode());
        final UcpRemoteKey rkey = ep.unpackRemoteKey(memory.getRemoteKeyBuffer());

        final int blockSize = UcpMemoryTest.MEM_SIZE / numRequests;
        for (int i = 0; i < numRequests; i++) {
            ep.getNonBlockingImplicit(memory.getAddress() + i * blockSize, rkey,
                    UcxUtils.getAddress(dst) + i * blockSize, blockSize);
        }

        final UcpRequest request = ep.flushNonBlocking(new UcxCallback() {
            @Override
            public void onSuccess(final UcpRequest request) {
                rkey.close();
                memory.deregister();
                assertEquals(dst.asCharBuffer().toString().trim(), UcpMemoryTest.RANDOM_TEXT);
            }
        });

        while (!request.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        Collections.addAll(resources, context2, context1, worker2, worker1, ep);
        closeResources();
    }

    @Test
    public void testRecvSize() throws Exception {
        final UcpContext context1 = new UcpContext(new UcpParams().requestTagFeature());
        final UcpContext context2 = new UcpContext(new UcpParams().requestTagFeature());

        final UcpWorker worker1 = context1.newWorker(new UcpWorkerParams());
        final UcpWorker worker2 = context2.newWorker(new UcpWorkerParams());

        final UcpEndpoint ep = worker1.newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()));

        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);

        sendBuffer.limit(UcpMemoryTest.MEM_SIZE / 2);

        final UcpRequest send = ep.sendTaggedNonBlocking(sendBuffer, null);
        final UcpRequest recv = worker2.recvTaggedNonBlocking(recvBuffer, null);

        while (!send.isCompleted() || !recv.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        assertEquals(UcpMemoryTest.MEM_SIZE / 2, recv.getRecvSize());

        Collections.addAll(resources, context1, context2, worker1, worker2, ep);
        closeResources();
    }

    @Test
    public void testStreamingAPI() throws Exception {
        final UcpParams params = new UcpParams().requestStreamFeature().requestRmaFeature();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);

        final UcpWorker worker1 = context1.newWorker(new UcpWorkerParams());
        final UcpWorker worker2 = context2.newWorker(new UcpWorkerParams());

        final UcpEndpoint clientToServer = worker1
                .newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()));

        final UcpEndpoint serverToClient = worker2
                .newEndpoint(new UcpEndpointParams().setUcpAddress(worker1.getAddress()));

        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        sendBuffer.put(0, (byte) 1);
        final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE * 2);

        final UcpRequest[] sends = new UcpRequest[2];

        sends[0] = clientToServer.sendStreamNonBlocking(sendBuffer, new UcxCallback() {
            @Override
            public void onSuccess(final UcpRequest request) {
                sendBuffer.put(0, (byte) 2);
                sends[1] = clientToServer.sendStreamNonBlocking(sendBuffer, null);
            }
        });

        while (sends[1] == null || !sends[1].isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        final AtomicBoolean received = new AtomicBoolean(false);
        serverToClient.recvStreamNonBlocking(UcxUtils.getAddress(recvBuffer), UcpMemoryTest.MEM_SIZE * 2L,
                UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
                    @Override
                    public void onSuccess(final UcpRequest request) {
                        assertEquals(request.getRecvSize(), UcpMemoryTest.MEM_SIZE * 2);
                        assertEquals((byte) 1, recvBuffer.get(0));
                        assertEquals((byte) 2, recvBuffer.get(UcpMemoryTest.MEM_SIZE));
                        received.set(true);
                    }
                });

        while (!received.get()) {
            worker1.progress();
            worker2.progress();
        }

        Collections.addAll(resources, context1, context2, worker1, worker2, clientToServer, serverToClient);
        closeResources();
    }

    @Test
    public void testIovOperations() throws Exception {
        final int NUM_IOV = 6;
        final long buffMultiplier = 10L;

        final UcpMemMapParams memMapParams = new UcpMemMapParams().allocate();
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestTagFeature().requestStreamFeature();
        final UcpWorkerParams workerParams = new UcpWorkerParams();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);
        final UcpWorker worker1 = context1.newWorker(workerParams);
        final UcpWorker worker2 = context2.newWorker(workerParams);

        final UcpEndpoint ep = worker1.newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()));

        final UcpEndpoint recvEp = worker2.newEndpoint(new UcpEndpointParams().setUcpAddress(worker1.getAddress()));

        final UcpMemory[] sendBuffers = new UcpMemory[NUM_IOV];
        final long[] sendAddresses = new long[NUM_IOV];
        final long[] sizes = new long[NUM_IOV];

        UcpMemory[] recvBuffers = new UcpMemory[NUM_IOV];
        long[] recvAddresses = new long[NUM_IOV];

        long totalSize = 0L;

        for (int i = 0; i < NUM_IOV; i++) {
            final long bufferSize = (i + 1) * buffMultiplier;
            totalSize += bufferSize;
            memMapParams.setLength(bufferSize);

            sendBuffers[i] = context1.memoryMap(memMapParams);
            sendAddresses[i] = sendBuffers[i].getAddress();
            sizes[i] = bufferSize;

            final ByteBuffer buf = UcxUtils.getByteBufferView(sendAddresses[i], (int) bufferSize);
            buf.putInt(0, (i + 1));

            recvBuffers[i] = context2.memoryMap(memMapParams);
            recvAddresses[i] = recvBuffers[i].getAddress();
        }

        ep.sendTaggedNonBlocking(sendAddresses, sizes, 0L, null);
        UcpRequest recv = worker2.recvTaggedNonBlocking(recvAddresses, sizes, 0L, 0L, null);

        while (!recv.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        assertEquals(totalSize, recv.getRecvSize());

        for (int i = 0; i < NUM_IOV; i++) {
            final ByteBuffer buf = UcxUtils.getByteBufferView(recvAddresses[i], (int) sizes[i]);
            assertEquals((i + 1), buf.getInt(0));
            recvBuffers[i].deregister();
        }

        // Test 6 send IOV to 3 recv IOV
        recvBuffers = new UcpMemory[NUM_IOV / 2];
        recvAddresses = new long[NUM_IOV / 2];
        final long[] recvSizes = new long[NUM_IOV / 2];
        totalSize = 0L;

        for (int i = 0; i < NUM_IOV / 2; i++) {
            final long bufferLength = (i + 1) * buffMultiplier * 2;
            totalSize += bufferLength;
            recvBuffers[i] = context2.memoryMap(memMapParams.setLength(bufferLength));
            recvAddresses[i] = recvBuffers[i].getAddress();
            recvSizes[i] = bufferLength;
        }

        ep.sendStreamNonBlocking(sendAddresses, sizes, null);
        recv = recvEp.recvStreamNonBlocking(recvAddresses, recvSizes, 0, null);

        while (!recv.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        assertEquals(totalSize, recv.getRecvSize());
        final ByteBuffer buf = UcxUtils.getByteBufferView(recvAddresses[0], (int) recvSizes[0]);
        assertEquals(1, buf.getInt(0));

        Collections.addAll(resources, context1, context2, worker1, worker2, ep);
        Collections.addAll(resources, sendBuffers);
        Collections.addAll(resources, recvBuffers);
        closeResources();
    }

    @Test
    public void testEpErrorHandler() throws Exception {
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestTagFeature();
        final UcpWorkerParams workerParams = new UcpWorkerParams();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);
        final UcpWorker worker1 = context1.newWorker(workerParams);
        final UcpWorker worker2 = context2.newWorker(workerParams);

        final ByteBuffer src = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer dst = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        src.asCharBuffer().put(UcpMemoryTest.RANDOM_TEXT);

        final AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);
        final UcpEndpointParams epParams = new UcpEndpointParams().setPeerErrorHandlingMode()
                .setErrorHandler((ep, status, errorMsg) -> {
                    errorHandlerCalled.set(true);
                    assertNotNull(errorMsg);
                })
                .setUcpAddress(worker2.getAddress());
        final UcpEndpoint ep = worker1.newEndpoint(epParams);

        final UcpRequest recv = worker2.recvTaggedNonBlocking(dst, null);
        final UcpRequest send = ep.sendTaggedNonBlocking(src, null);

        while (!send.isCompleted() || !recv.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        // Closing receiver worker & context
        worker2.close();
        context2.close();
        assertNull(context2.getNativeId());

        final AtomicBoolean errorCallabackCalled = new AtomicBoolean(false);

        ep.sendTaggedNonBlocking(src, null);
        worker1.progressRequest(ep.flushNonBlocking(new UcxCallback() {
            @Override
            public void onError(final int ucsStatus, final String errorMsg) {
                errorCallabackCalled.set(true);
            }
        }));

        assertTrue(errorHandlerCalled.get());
        assertTrue(errorCallabackCalled.get());

        ep.close();
        worker1.close();
        context1.close();
    }

    @Test
    public void testActiveMessages() throws Exception {
        for (final int memType : memTypes()) {
            System.out.println("Running testActiveMessages with memType: " + memType);
            final UcpParams params = new UcpParams().requestAmFeature().requestTagFeature();
            final UcpContext context1 = new UcpContext(params);
            final UcpContext context2 = new UcpContext(params);

            final UcpWorker worker1 = context1.newWorker(new UcpWorkerParams());
            final UcpWorker worker2 = context2.newWorker(new UcpWorkerParams());

            final String headerString = "Hello";
            final String dataString = "Active messages";
            final long headerSize = headerString.length() * 2;
            final long dataSize = UcpMemoryTest.MEM_SIZE;
            assertTrue(headerSize < worker1.getMaxAmHeaderSize());

            final ByteBuffer header = ByteBuffer.allocateDirect((int) headerSize);
            header.asCharBuffer().append(headerString);

            header.rewind();

            final MemoryBlock sendData = allocateMemory(context2, worker2, memType, dataSize);
            sendData.setData(dataString);

            final MemoryBlock recvData = allocateMemory(context1, worker1, memType, dataSize);
            final MemoryBlock recvEagerData = allocateMemory(context1, worker1, memType, dataSize);
            final ByteBuffer recvHeader = ByteBuffer.allocateDirect((int) headerSize);
            final UcpRequest[] requests = new UcpRequest[7];

            final UcpEndpoint ep = worker2.newEndpoint(new UcpEndpointParams().setUcpAddress(worker1.getAddress()));

            final Set<UcpEndpoint> cachedEp = new HashSet<>();

            // Test rndv flow
            worker1.setAmRecvHandler(0, (headerAddress, headerSize12, amData, replyEp) -> {
                assertFalse(amData.isDataValid());
                try {
                    assertEquals(headerString,
                            UcxUtils.getByteBufferView(headerAddress, (int) headerSize12)
                                    .asCharBuffer()
                                    .toString()
                                    .trim());
                } catch (final Exception e) {
                    e.printStackTrace();
                }

                requests[2] = replyEp.sendTaggedNonBlocking(header, null);
                requests[3] = amData.receive(recvData.getMemory().getAddress(), null);

                if (!cachedEp.isEmpty()) {
                    assertTrue(cachedEp.contains(replyEp));
                } else {
                    cachedEp.add(replyEp);
                }

                return UcsConstants.STATUS.UCS_OK;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);

            // Test eager flow
            worker1.setAmRecvHandler(1, (headerAddress, headerSize1, amData, replyEp) -> {
                assertTrue(amData.isDataValid());
                try {
                    assertEquals(dataString,
                            UcxUtils.getByteBufferView(amData.getDataAddress(), (int) amData.getLength())
                                    .asCharBuffer()
                                    .toString()
                                    .trim());
                } catch (final Exception e) {
                    e.printStackTrace();
                }

                if (!cachedEp.isEmpty()) {
                    assertTrue(cachedEp.contains(replyEp));
                } else {
                    cachedEp.add(replyEp);
                }

                requests[6] = amData.receive(recvEagerData.getMemory().getAddress(), null);

                return UcsConstants.STATUS.UCS_OK;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG);

            final AtomicReference<UcpAmData> persistantAmData = new AtomicReference<>(null);
            // Test amData persistence flow
            worker1.setAmRecvHandler(2, (headerAddress, headerSize1, amData, replyEp) -> {
                assertTrue(amData.isDataValid());
                assertTrue(amData.canPersist());
                persistantAmData.set(amData);
                return UcsConstants.STATUS.UCS_INPROGRESS;
            }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG | UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA);

            requests[0] = ep.sendAmNonBlocking(0, UcxUtils.getAddress(header), headerSize,
                    sendData.getMemory().getAddress(), sendData.getMemory().getLength(),
                    UcpConstants.UCP_AM_SEND_FLAG_REPLY | UcpConstants.UCP_AM_SEND_FLAG_RNDV, new UcxCallback() {
                        @Override
                        public void onSuccess(final UcpRequest request) {
                            assertTrue(request.isCompleted());
                        }
                    }, new UcpRequestParams().setMemoryType(memType).setMemoryHandle(sendData.getMemory()));

            requests[1] = worker2.recvTaggedNonBlocking(recvHeader, null);
            requests[4] = ep.sendAmNonBlocking(1, 0L, 0L, sendData.getMemory().getAddress(), dataSize,
                    UcpConstants.UCP_AM_SEND_FLAG_REPLY | UcpConstants.UCP_AM_SEND_FLAG_EAGER, null,
                    new UcpRequestParams().setMemoryType(memType).setMemoryHandle(sendData.getMemory()));

            // Persistence data flow
            requests[5] = ep.sendAmNonBlocking(2, 0L, 0L, sendData.getMemory().getAddress(), 2L,
                    UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA, null,
                    new UcpRequestParams().setMemoryType(memType).setMemoryHandle(sendData.getMemory()));

            while (!Arrays.stream(requests).allMatch(r -> (r != null) && r.isCompleted())) {
                worker1.progress();
                worker2.progress();
            }

            assertEquals(dataString, recvData.getData().asCharBuffer().toString().trim());

            assertEquals(dataString, recvEagerData.getData().asCharBuffer().toString().trim());

            assertEquals(headerString, recvHeader.asCharBuffer().toString().trim());

            assertEquals(dataString.charAt(0), UcxUtils
                    .getByteBufferView(persistantAmData.get().getDataAddress(), persistantAmData.get().getLength())
                    .getChar(0));
            persistantAmData.get().close();
            persistantAmData.set(null);

            // Reset AM callback
            worker1.removeAmRecvHandler(0);
            worker1.removeAmRecvHandler(1);
            worker1.removeAmRecvHandler(2);

            Collections.addAll(resources, context1, context2, worker1, worker2, ep, cachedEp.iterator().next(),
                    sendData, recvData, recvEagerData);
            closeResources();
            cachedEp.clear();
        }
    }

}