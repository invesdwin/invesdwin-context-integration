package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.XAddParams;

@NotThreadSafe
public class JedisXaddSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    protected final JedisSynchronousChannel channel;
    protected final String topic;
    protected final byte[] topicBytes;
    protected RedisClient client;
    protected XAddParams params;
    protected final boolean flush;
    protected Pipeline pipe;
    protected int pipeSize;
    protected final int flushSize;

    public JedisXaddSynchronousWriter(final JedisSynchronousChannel channel, final String topic, final boolean flush) {
        this.channel = channel;
        this.topic = topic;
        this.topicBytes = topic.getBytes();
        this.params = newParams();
        this.flush = flush;
        this.flushSize = newFlushSize();
    }

    protected int newFlushSize() {
        return 500;
    }

    protected XAddParams newParams() {
        return new XAddParams();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        client = channel.getJedisClient();
        if (!flush) {
            this.pipe = client.pipelined();
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            write(ClosedByteBuffer.INSTANCE);
            if (!flush) {
                pipe.close();
                pipe = null;
            }
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
        if (!flush) {
            this.pipe.xadd(topicBytes, params, Map.of(Bytes.EMPTY_ARRAY, messageBytes));
            this.pipeSize += 1;
            if (this.pipeSize >= this.flushSize) {
                this.pipe.sync();
                this.pipeSize = 0;
            }
        } else {
            client.xadd(topicBytes, params, Map.of(Bytes.EMPTY_ARRAY, messageBytes));
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }
}