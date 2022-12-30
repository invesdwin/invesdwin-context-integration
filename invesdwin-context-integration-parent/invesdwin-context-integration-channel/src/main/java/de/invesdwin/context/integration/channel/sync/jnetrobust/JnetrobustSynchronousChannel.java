package de.invesdwin.context.integration.channel.sync.jnetrobust;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.mucaho.jnetrobust.example.ProtocolHandle;
import com.github.mucaho.jnetrobust.example.ProtocolHost;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class JnetrobustSynchronousChannel implements ISynchronousChannel {

    private final InetSocketAddress ourAddress;
    private final InetSocketAddress otherAddress;
    private volatile ProtocolHost protocolHost;
    private volatile ProtocolHandle<byte[]> protocolHandle;
    private boolean writerRegistered;
    private boolean readerRegistered;

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public JnetrobustSynchronousChannel(final InetSocketAddress ourAddress, final InetSocketAddress otherAddress) {
        this.ourAddress = ourAddress;
        this.otherAddress = otherAddress;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitProtocolHandle();
            return;
        }
        this.protocolHost = new ProtocolHost(null, ourAddress, byte[].class);
        this.protocolHandle = protocolHost.register(Byte.MIN_VALUE, otherAddress);
    }

    private void awaitProtocolHandle() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((protocolHandle == null) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new IOException(t);
        }
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected Duration getWaitInterval() {
        return SynchronousChannels.DEFAULT_WAIT_INTERVAL;
    }

    public ProtocolHandle<byte[]> getProtocolHandle() {
        return protocolHandle;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        if (protocolHost != null) {
            protocolHost.getChannel().close();
            protocolHost = null;
        }
    }

    public void registerWriter() {
        this.writerRegistered = true;
    }

    public boolean isWriterRegistered() {
        return writerRegistered;
    }

    public void registerReader() {
        this.readerRegistered = true;
    }

    public boolean isReaderRegistered() {
        return readerRegistered;
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

}
