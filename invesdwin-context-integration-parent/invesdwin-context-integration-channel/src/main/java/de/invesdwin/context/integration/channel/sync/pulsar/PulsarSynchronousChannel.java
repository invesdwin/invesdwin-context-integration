package de.invesdwin.context.integration.channel.sync.pulsar;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.BytesSchema;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.closeable.Closeables;

@ThreadSafe
public class PulsarSynchronousChannel implements ISynchronousChannel {

    static {
        //workaround for deadlocks during statisic initialization of pulsar
        Assertions.checkNotNull(BytesSchema.of());
    }

    private final String serviceUrl;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final PulsarSynchronousChannelFinalizer finalizer;

    public PulsarSynchronousChannel(final String serviceUrl) {
        this.serviceUrl = serviceUrl;
        this.finalizer = new PulsarSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected PulsarClient newPulsarClient() throws PulsarClientException {
        return newPulsarClientBuilder().build();
    }

    protected ClientBuilder newPulsarClientBuilder() {
        return PulsarClient.builder().serviceUrl(serviceUrl);
    }

    public PulsarClient getPulsarClient() {
        return finalizer.pulsarClient;
    }

    @Override
    public synchronized void open() throws IOException {
        if (!shouldOpen()) {
            return;
        }
        finalizer.pulsarClient = newPulsarClient();
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    @Override
    public void close() throws IOException {
        if (!shouldClose()) {
            return;
        }
        finalizer.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private static final class PulsarSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile PulsarClient pulsarClient;

        @Override
        protected void clean() {
            final PulsarClient pulsarClientCopy = pulsarClient;
            if (pulsarClientCopy != null) {
                pulsarClient = null;
                Closeables.closeQuietly(pulsarClientCopy);
            }
        }

        @Override
        protected boolean isCleaned() {
            return pulsarClient == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
