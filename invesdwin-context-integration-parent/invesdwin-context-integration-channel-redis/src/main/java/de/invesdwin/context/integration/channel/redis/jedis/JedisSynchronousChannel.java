package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.closeable.Closeables;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.builders.StandaloneClientBuilder;

@ThreadSafe
public class JedisSynchronousChannel implements ISynchronousChannel {

    private final InetSocketAddress serverAddress;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final JedisSynchronousChannelFinalizer finalizer;

    public JedisSynchronousChannel(final InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.finalizer = new JedisSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }

    protected RedisClient newJedisClient() {
        return newJedisClientBuilder().build();
    }

    protected StandaloneClientBuilder<RedisClient> newJedisClientBuilder() {
        return RedisClient.builder().hostAndPort(serverAddress.getHostName(), serverAddress.getPort());
    }

    public RedisClient getJedisClient() {
        return finalizer.jedisClient;
    }

    @Override
    public synchronized void open() throws IOException {
        if (!shouldOpen()) {
            return;
        }
        finalizer.jedisClient = newJedisClient();
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

    private static final class JedisSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile RedisClient jedisClient;

        @Override
        protected void clean() {
            final RedisClient jedisClientCopy = jedisClient;
            if (jedisClientCopy != null) {
                jedisClient = null;
                Closeables.closeQuietly(jedisClientCopy);
            }
        }

        @Override
        protected boolean isCleaned() {
            return jedisClient == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
