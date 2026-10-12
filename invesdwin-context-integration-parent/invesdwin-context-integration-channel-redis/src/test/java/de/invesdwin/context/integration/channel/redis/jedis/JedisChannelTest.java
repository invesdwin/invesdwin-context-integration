package de.invesdwin.context.integration.channel.redis.jedis;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class JedisChannelTest extends AChannelTest {
    private static final int REDIS_PORT = 6379;
    @Container
    private static final GenericContainer<?> REDIS_CONTAINER = newRedisContainer();
    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    @SuppressWarnings("resource")
    private static GenericContainer<?> newRedisContainer() {
        return new GenericContainer<>(DockerImageName.parse("redis:8.8.3-alpine")).withExposedPorts(REDIS_PORT);
        // dragonflydb/dragonfly:v1.27.1
        // redis:7.2-alpine
    }

    private InetSocketAddress newRedisServerAddress() {
        //        return InetSocketAddress.createUnresolved("localhost", REDIS_PORT);
        return InetSocketAddress.createUnresolved(REDIS_CONTAINER.getHost(), REDIS_CONTAINER.getMappedPort(REDIS_PORT));
    }

    @Test
    public void testRedisLatency() throws InterruptedException {
        final InetSocketAddress redisServerAddress = newRedisServerAddress();
        final String responseTopic = "testRedisLatency_response";
        final String requestTopic = "testRedisLatency_request";
        deleteTopics(redisServerAddress, requestTopic, responseTopic);
        runRedisLatencyTest(redisServerAddress, responseTopic, requestTopic);
    }

    @Test
    public void testRedisThroughput() throws InterruptedException {
        final InetSocketAddress redisServerAddress = newRedisServerAddress();
        final String topic = "testRedisThroughput_channel";
        deleteTopics(redisServerAddress, topic);
        runRedisThroughputTest(redisServerAddress, topic);
    }

    private void deleteTopics(final InetSocketAddress redisServerAddress, final String... topics) {
        try (JedisSynchronousChannel channel = new JedisSynchronousChannel(redisServerAddress)) {
            channel.open();
            for (int i = 0; i < topics.length; i++) {
                final String topic = topics[i];
                channel.getJedisClient().del(topic);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void runRedisThroughputTest(final InetSocketAddress redisServerAddress, final String topic)
            throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newJedisSynchronousWriter(new JedisSynchronousChannel(redisServerAddress), topic));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                newJedisSynchronousReader(new JedisSynchronousChannel(redisServerAddress), topic));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runRedisLatencyTest(final InetSocketAddress redisServerAddress, final String responseTopic,
            final String requestTopic) throws InterruptedException {
        final JedisSynchronousChannel serverChannel = new JedisSynchronousChannel(redisServerAddress);
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                newJedisSynchronousReader(serverChannel, requestTopic));
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newJedisSynchronousWriter(serverChannel, responseTopic));
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);

        final JedisSynchronousChannel clientChannel = new JedisSynchronousChannel(redisServerAddress);
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                newJedisSynchronousReader(clientChannel, responseTopic));
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newJedisSynchronousWriter(clientChannel, requestTopic));
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);

        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    private ISynchronousReader<IByteBufferProvider> newJedisSynchronousReader(final JedisSynchronousChannel channel,
            final String topic) {
        return new JedisXreadSynchronousReader(channel, topic) {
            @Override
            protected Duration newPollTimeout() {
                return POLL_TIMEOUT;
            }
        };
    }

    protected ISynchronousWriter<IByteBufferProvider> newJedisSynchronousWriter(final JedisSynchronousChannel channel,
            final String topic) {
        return new JedisXaddSynchronousWriter(channel, topic);
    }
}
