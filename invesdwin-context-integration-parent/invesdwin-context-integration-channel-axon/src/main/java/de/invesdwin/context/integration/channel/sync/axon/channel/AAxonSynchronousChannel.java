package de.invesdwin.context.integration.channel.sync.axon.channel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.config.Configuration;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;

@ThreadSafe
public abstract class AAxonSynchronousChannel implements ISynchronousChannel {

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final AxonSynchronousChannelFinalizer finalizer;

    public AAxonSynchronousChannel() {
        this.finalizer = new AxonSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public Configuration getConfiguration() {
        return finalizer.configuration;
    }

    @Override
    public synchronized void open() throws IOException {
        if (activeCount.incrementAndGet() != 1) {
            return;
        }
        finalizer.configuration = newConfiguration();
        finalizer.configuration.start();
    }

    protected abstract Configuration newConfiguration();

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

    private static final class AxonSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile Configuration configuration;

        @Override
        protected void clean() {
            final Configuration configCopy = configuration;
            if (configCopy != null) {
                configuration = null;
                configCopy.shutdown();
            }
        }

        @Override
        protected boolean isCleaned() {
            return configuration == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}