package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.collections.array.primitive.circular.CircularGenericPrimitiveArrayQueue;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.queue.IGenericQueue;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntryBinary;

@NotThreadSafe
public class JedisXreadSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final Duration DEFAULT_POLL_TIMEOUT = StreamSynchronousEndpointClientReader.DEFAULT_POLL_TIMEOUT;

    protected final JedisSynchronousChannel channel;
    protected final String topic;
    protected byte[] topicBytes;
    protected final Duration pollTimeout;
    private RedisClient client;
    private final XReadParams params;
    private final Map<byte[], StreamEntryID> streams;
    private final Entry<byte[], StreamEntryID> streamEntry;

    // Use a queue to buffer messages fetched in batches
    private final IGenericQueue<StreamEntryBinary> messageBuffer;
    private StreamEntryBinary message;

    public JedisXreadSynchronousReader(final JedisSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
        this.topicBytes = topic.getBytes();
        this.pollTimeout = newPollTimeout();
        this.params = newParams();
        this.streams = ILockCollectionFactory.getInstance(false).newMap(1);
        this.streams.put(topicBytes, newStartMessageId());
        this.streamEntry = streams.entrySet().iterator().next();
        this.messageBuffer = new CircularGenericPrimitiveArrayQueue<StreamEntryBinary>(newBatchSize());
    }

    protected XReadParams newParams() {
        // Read multiple values per poll to increase throughput
        return new XReadParams().count(newBatchSize()).block(pollTimeout.intValue(FTimeUnit.MILLISECONDS));
    }

    protected int newBatchSize() {
        return DEFAULT_BATCH_SIZE;
    }

    protected Duration newPollTimeout() {
        return DEFAULT_POLL_TIMEOUT;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        client = channel.getJedisClient();
    }

    protected StreamEntryID newStartMessageId() {
        return new StreamEntryID();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (message != null) {
            return true;
        }

        // Fetch a new batch from Redis only if the buffer is empty
        if (messageBuffer.isEmpty()) {
            final List<Entry<byte[], List<StreamEntryBinary>>> entries = client.xreadBinary(params, streams);
            if (entries == null || entries.isEmpty()) {
                return false;
            }
            assert entries.size() == 1 : "entries.size[" + entries.size() + "] != 1";
            final Entry<byte[], List<StreamEntryBinary>> entry = entries.get(0);
            final List<StreamEntryBinary> values = entry.getValue();
            if (values == null || values.isEmpty()) {
                return false;
            }
            for (int i = 0; i < values.size(); i++) {
                final StreamEntryBinary message = values.get(i);
                messageBuffer.add(message);
            }
        }

        // Retrieve the next message from the local buffer
        message = messageBuffer.poll();
        return message != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final byte[] msg = message.getFields().get(Bytes.EMPTY_ARRAY);
        if (ClosedByteBuffer.isClosed(msg)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return ByteBuffers.wrap(msg);
    }

    @Override
    public void readFinished() throws IOException {
        // Update the offset tracking to the latest message processed
        // This ensures the next xreadBinary call starts after this exact ID
        streamEntry.setValue(message.getID());
        message = null;
    }
}