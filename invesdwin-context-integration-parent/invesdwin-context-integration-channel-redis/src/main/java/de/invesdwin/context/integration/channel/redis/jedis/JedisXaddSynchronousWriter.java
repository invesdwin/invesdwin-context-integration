package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.XAddParams;

@NotThreadSafe
public class JedisXaddSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    protected final JedisSynchronousChannel channel;
    protected final String topic;
    protected final byte[] topicBytes;
    protected RedisClient client;
    protected XAddParams params;

    public JedisXaddSynchronousWriter(final JedisSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
        this.topicBytes = topic.getBytes();
        this.params = newParams();
    }

    protected XAddParams newParams() {
        return new XAddParams();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        client = channel.getJedisClient();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            write(ClosedByteBuffer.INSTANCE);
            client = null;
            channel.close();
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return client != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final byte[] messageBytes = message.asBuffer().asByteArrayCopy();
        client.xadd(topicBytes, params, Map.of(Bytes.EMPTY_ARRAY, messageBytes));
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}