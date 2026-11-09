package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.collections.array.primitive.queue.IGenericPrimitiveQueue;
import de.invesdwin.util.collections.array.primitive.queue.circular.CircularGenericPrimitiveArrayQueue;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntryBinary;

@NotThreadSafe
public class JedisXreadgroupSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final Duration DEFAULT_POLL_TIMEOUT = StreamSynchronousEndpointClientReader.DEFAULT_POLL_TIMEOUT;

    protected final JedisSynchronousChannel channel;
    protected final String topic;
    protected byte[] topicBytes;
    protected final Duration pollTimeout;
    private RedisClient client;
    private final XReadGroupParams params;
    private final Map<byte[], StreamEntryID> streams;

    // Use a queue to buffer messages fetched in batches
    private final IGenericPrimitiveQueue<StreamEntryBinary> messageBuffer;
    private StreamEntryBinary message;

    private final String groupName;
    private final byte[] groupNameBytes;
    private final String consumerName;
    private final byte[] consumerNameBytes;

    public JedisXreadgroupSynchronousReader(final JedisSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
        this.topicBytes = topic.getBytes();
        this.pollTimeout = newPollTimeout();
        this.params = newParams();
        this.streams = ILockCollectionFactory.getInstance(false).newMap(1);
        this.streams.put(topicBytes, newStartMessageId());
        this.messageBuffer = new CircularGenericPrimitiveArrayQueue<StreamEntryBinary>(newBatchSize());
        this.groupName = this.getGroupName();
        this.groupNameBytes = this.groupName.getBytes();
        this.consumerName = this.getConsumerName();
        this.consumerNameBytes = this.consumerName.getBytes();

    }

    protected String getConsumerName() {
        return this.groupName + "_consumer_1";
    }

    protected String getGroupName() {
        return this.topic + "_consumerGroup";
    }

    protected XReadGroupParams newParams() {

        if (pollTimeout.isZero()) {
            // Redis treats BLOCK 0 as blocking indefinitely
            return new XReadGroupParams().noAck().count(newBatchSize());
        }
        // Read multiple values per poll to increase throughput
        return new XReadGroupParams().count(newBatchSize()).block(pollTimeout.intValue(FTimeUnit.MILLISECONDS)).noAck();
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

        client.xgroupCreate(this.topicBytes, this.groupNameBytes, this.newGroupStartId().toString().getBytes(), true);
    }

    protected StreamEntryID newGroupStartId() {
        return new StreamEntryID();
    }

    protected StreamEntryID newStartMessageId() {
        return StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
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
            final List<Map.Entry<byte[], List<StreamEntryBinary>>> entries = client.xreadGroupBinary(groupNameBytes,
                    consumerNameBytes, params, streams);
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
        message = null;
    }
}