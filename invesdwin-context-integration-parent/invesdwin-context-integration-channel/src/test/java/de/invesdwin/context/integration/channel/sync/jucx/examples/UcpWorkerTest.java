// @NotThreadSafe
// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.jucx.examples;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpTagMessage;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

public class UcpWorkerTest extends UcxTest {
    private static int numWorkers = Runtime.getRuntime().availableProcessors();

    @Test
    public void testSingleWorker() throws Exception {
        final UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
        Assertions.assertEquals(2, UcsConstants.ThreadMode.UCS_THREAD_MODE_MULTI);
        Assertions.assertNotEquals(context.getNativeId(), null);
        final UcpWorker worker = context.newWorker(new UcpWorkerParams());
        Assertions.assertNotNull(worker.getNativeId());
        Assertions.assertEquals(0, worker.progress()); // No communications was submitted.
        worker.close();
        Assertions.assertNull(worker.getNativeId());
        context.close();
    }

    @Test
    public void testMultipleWorkersWithinSameContext() {
        final UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
        Assertions.assertNotEquals(context.getNativeId(), null);
        final UcpWorker[] workers = new UcpWorker[numWorkers];
        final UcpWorkerParams workerParam = new UcpWorkerParams();
        for (int i = 0; i < numWorkers; i++) {
            workerParam.clear().setCpu(i).requestThreadSafety();
            workers[i] = context.newWorker(workerParam);
            Assertions.assertNotNull(workers[i].getNativeId());
        }
        for (int i = 0; i < numWorkers; i++) {
            workers[i].close();
        }
        context.close();
    }

    @Test
    public void testMultipleWorkersFromMultipleContexts() {
        final UcpContext tcpContext = new UcpContext(new UcpParams().requestTagFeature());
        final UcpContext rdmaContext = new UcpContext(
                new UcpParams().requestRmaFeature().requestAtomic64BitFeature().requestAtomic32BitFeature());
        final UcpWorker[] workers = new UcpWorker[numWorkers];
        final UcpWorkerParams workerParams = new UcpWorkerParams();
        for (int i = 0; i < numWorkers; i++) {
            final ByteBuffer userData = ByteBuffer.allocateDirect(100);
            workerParams.clear();
            if (i % 2 == 0) {
                userData.asCharBuffer().put("TCPWorker" + i);
                workerParams.requestWakeupRX().setUserData(userData);
                workers[i] = tcpContext.newWorker(workerParams);
            } else {
                userData.asCharBuffer().put("RDMAWorker" + i);
                workerParams.requestWakeupRMA().setCpu(i).setUserData(userData).requestThreadSafety();
                workers[i] = rdmaContext.newWorker(workerParams);
            }
        }
        for (int i = 0; i < numWorkers; i++) {
            workers[i].close();
        }
        tcpContext.close();
        rdmaContext.close();
    }

    @Test
    public void testGetWorkerAddress() {
        final UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
        final UcpWorker worker = context.newWorker(new UcpWorkerParams());
        final ByteBuffer workerAddress = worker.getAddress();
        Assertions.assertNotNull(workerAddress);
        Assertions.assertTrue(workerAddress.capacity() > 0);
        worker.close();
        context.close();
    }

    @Test
    public void testWorkerArmAndGetEventFD() {
        final UcpContext context = new UcpContext(new UcpParams().requestRmaFeature().requestWakeupFeature());
        final UcpWorker worker = context.newWorker(new UcpWorkerParams());

        final int eventFD = worker.getEventFD();
        Assertions.assertNotEquals(0, eventFD);

        worker.arm(); // Test passes, if no exception is thrown

        worker.close();
        context.close();
    }

    @Test
    public void testWorkerSleepWakeup() throws InterruptedException {
        final UcpContext context = new UcpContext(new UcpParams().requestRmaFeature().requestWakeupFeature());
        final UcpWorker worker = context.newWorker(new UcpWorkerParams().requestWakeupRMA());

        final AtomicBoolean success = new AtomicBoolean(false);
        final Thread workerProgressThread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        if (worker.progress() == 0) {
                            worker.waitForEvents();
                        }
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                }
                success.set(true);
            }
        };

        workerProgressThread.start();

        workerProgressThread.interrupt();
        worker.signal();

        workerProgressThread.join();
        Assertions.assertTrue(success.get());

        worker.close();
        context.close();
    }

    @Test
    public void testFlushWorker() throws Exception {
        final int numRequests = 10;
        // Create 2 contexts + 2 workers
        final UcpParams params = new UcpParams().requestRmaFeature();
        final UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);

        final ByteBuffer src = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        final ByteBuffer dst = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);
        dst.asCharBuffer().put(UcpMemoryTest.RANDOM_TEXT);
        final UcpMemory memory = context2.registerMemory(src);

        final UcpWorker worker1 = context1.newWorker(rdmaWorkerParams);
        final UcpWorker worker2 = context2.newWorker(rdmaWorkerParams);

        final UcpEndpoint ep = worker1
                .newEndpoint(new UcpEndpointParams().setUcpAddress(worker2.getAddress()).setPeerErrorHandlingMode());
        final UcpRemoteKey rkey = ep.unpackRemoteKey(memory.getRemoteKeyBuffer());

        final int blockSize = UcpMemoryTest.MEM_SIZE / numRequests;
        for (int i = 0; i < numRequests; i++) {
            ep.putNonBlockingImplicit(UcxUtils.getAddress(dst) + i * blockSize, blockSize,
                    memory.getAddress() + i * blockSize, rkey);
        }

        final UcpRequest request = worker1.flushNonBlocking(new UcxCallback() {
            @Override
            public void onSuccess(final UcpRequest request) {
                rkey.close();
                memory.deregister();
                Assertions.assertEquals(dst.asCharBuffer().toString().trim(), UcpMemoryTest.RANDOM_TEXT);
            }
        });

        while (!request.isCompleted()) {
            worker1.progress();
            worker2.progress();
        }

        Assertions.assertTrue(request.isCompleted());
        Collections.addAll(resources, context1, context2, worker1, worker2, ep);
        closeResources();
    }

    @Test
    public void testTagProbe() throws Exception {
        final UcpParams params = new UcpParams().requestTagFeature();
        final UcpContext context1 = new UcpContext(params);
        final UcpContext context2 = new UcpContext(params);

        final UcpWorker worker1 = context1.newWorker(new UcpWorkerParams());
        final UcpWorker worker2 = context2.newWorker(new UcpWorkerParams());
        final ByteBuffer recvBuffer = ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE);

        UcpTagMessage message = worker1.tagProbeNonBlocking(0, 0, false);

        Assertions.assertNull(message);

        final UcpEndpoint endpoint = worker2.newEndpoint(new UcpEndpointParams().setUcpAddress(worker1.getAddress()));

        endpoint.sendTaggedNonBlocking(ByteBuffer.allocateDirect(UcpMemoryTest.MEM_SIZE), null);

        do {
            worker1.progress();
            worker2.progress();
            message = worker1.tagProbeNonBlocking(0, 0, true);
        } while (message == null);

        Assertions.assertEquals(UcpMemoryTest.MEM_SIZE, message.getRecvLength());
        Assertions.assertEquals(0, message.getSenderTag());

        final UcpRequest recv = worker1.recvTaggedMessageNonBlocking(recvBuffer, message, null);

        worker1.progressRequest(recv);

        Collections.addAll(resources, context1, context2, worker1, worker2, endpoint);
    }
}
