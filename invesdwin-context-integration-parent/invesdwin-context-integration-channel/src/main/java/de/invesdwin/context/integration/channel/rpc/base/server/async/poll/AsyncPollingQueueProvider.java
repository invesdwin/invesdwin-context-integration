package de.invesdwin.context.integration.channel.rpc.base.server.async.poll;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.server.async.AsynchronousEndpointServerHandler;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class AsyncPollingQueueProvider implements IPollingQueueProvider {

    public static final AsyncPollingQueueProvider INSTANCE = new AsyncPollingQueueProvider();

    private static final ManyToOneConcurrentLinkedQueue<ProcessResponseResult> POLLING_QUEUE_ADDS = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("POLLING_QUEUE_ADDS")
    private static WrappedExecutorService pollingExecutor;

    private AsyncPollingQueueProvider() {}

    @Override
    public void addToPollingQueue(final ProcessResponseResult result) {
        addToPollingQueueStatic(result);
    }

    private static void addToPollingQueueStatic(final ProcessResponseResult result) {
        synchronized (POLLING_QUEUE_ADDS) {
            if (pollingExecutor == null) {
                //reduce cpu load by using max 1 thread
                pollingExecutor = Executors
                        .newFixedThreadPool(AsynchronousEndpointServerHandler.class.getSimpleName() + "_POLLING", 1)
                        .setDynamicThreadName(false);
                pollingExecutor.execute(new PollingQueueRunnable());
            }
            POLLING_QUEUE_ADDS.add(result);
        }
    }

    private static final class PollingQueueRunnable implements Runnable {

        private final ASpinWait spinWait = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return maybePollResults();
            }
        };
        private long lastChangeNanos = System.nanoTime();
        private final NodeBufferingIterator<ProcessResponseResult> pollingQueue = new NodeBufferingIterator<>();

        @Override
        public void run() {
            try {
                while (true) {
                    if (!spinWait.awaitFulfill(System.nanoTime(), Duration.ONE_MINUTE)) {
                        if (maybeClosePollingExecutor()) {
                            return;
                        }
                    }
                }
            } catch (final InterruptedException e) {
                //ignore
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        private boolean maybeClosePollingExecutor() {
            if (isTimeout()) {
                synchronized (POLLING_QUEUE_ADDS) {
                    if (isTimeout()) {
                        pollingExecutor.shutdown();
                        pollingExecutor = null;
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isTimeout() {
            return pollingQueue.isEmpty() && POLLING_QUEUE_ADDS.isEmpty()
                    && Duration.TEN_MINUTES.isLessThanNanos(System.nanoTime() - lastChangeNanos);
        }

        private boolean maybePollResults() {
            boolean changed = false;
            if (!POLLING_QUEUE_ADDS.isEmpty()) {
                ProcessResponseResult addPollingResult = POLLING_QUEUE_ADDS.poll();
                while (addPollingResult != null) {
                    pollingQueue.add(addPollingResult);
                    changed = true;
                    addPollingResult = POLLING_QUEUE_ADDS.poll();
                    lastChangeNanos = System.nanoTime();
                }
            }
            if (!pollingQueue.isEmpty()) {
                ProcessResponseResult pollingResult = pollingQueue.getHead();
                while (pollingResult != null) {
                    final ProcessResponseResult nextPollingResult = pollingResult.getNext();
                    if (pollingResult.isDone()) {
                        if (pollingResult.isDelayedWriteResponse()) {
                            pollingResult.getContext().write(pollingResult.getResponse().asBuffer());
                        }
                        pollingQueue.remove(pollingResult);
                        changed = true;
                        pollingResult.close();
                        lastChangeNanos = System.nanoTime();
                    }
                    pollingResult = nextPollingResult;
                }
            }
            return changed;
        }
    }

}
