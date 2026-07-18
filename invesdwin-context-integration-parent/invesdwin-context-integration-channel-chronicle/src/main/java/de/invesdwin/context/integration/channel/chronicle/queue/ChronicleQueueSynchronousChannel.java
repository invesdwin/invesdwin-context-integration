package de.invesdwin.context.integration.channel.chronicle.queue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

@ThreadSafe
public class ChronicleQueueSynchronousChannel implements ISynchronousChannel {

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final ChronicleQueueSynchronousChannelFinalizer finalizer;

    public ChronicleQueueSynchronousChannel(final File file) {
        this.finalizer = new ChronicleQueueSynchronousChannelFinalizer(file);
        finalizer.register(this);
    }

    public ChronicleQueue getQueue() {
        return finalizer.queue;
    }

    @Override
    public synchronized void open() throws IOException {
        if (activeCount.incrementAndGet() != 1) {
            return;
        }
        finalizer.queue = newQueue();
    }

    protected ChronicleQueue newQueue() {
        try {
            return SingleChronicleQueueBuilder.binary(finalizer.file).rollCycle(RollCycles.FIVE_MINUTELY).build();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to open file: " + finalizer.file, e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        if (activeCountBefore == 1) {
            finalizer.close();
        }
    }

    private static final class ChronicleQueueSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile ChronicleQueue queue;
        private final File file;

        private ChronicleQueueSynchronousChannelFinalizer(final File file) {
            this.file = file;
        }

        @Override
        protected void clean() {
            if (queue != null) {
                try {
                    queue.close();
                    queue = null;
                } catch (final Exception e) {
                    throw new RuntimeException("Unable to close the file: " + file, e);
                }
            }
        }

        @Override
        protected boolean isCleaned() {
            return queue == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}