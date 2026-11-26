package de.invesdwin.context.integration.channel.redis.examples;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

@Testcontainers
@NotThreadSafe
public class JedisRedisSendReceiveExampleTest extends ATest {

    private static final String STREAM_NAME = "helloworldtopic";
    private static final String GROUP_NAME = "test_consumer_group";
    private static final String CONSUMER_ID = "c1";
    private static final String MESSAGE = "helloworldtest";
    private static final long NUMOFEVENTS = AChannelTest.MESSAGE_COUNT;

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);
    // dragonflydb/dragonfly:v1.27.1
    // redis:7.2-alpine
    private final RedisClient client = RedisClient.builder()
            .hostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379))
            .build();

    @Test
    public void test() throws InterruptedException {
        //checks connection is good by pinging server and then checking K/V pairs
        Assertions.checkEquals("PONG", client.ping());
        client.set("hello", "world");
        Assertions.checkEquals("world", client.get("hello"));

        try {
            client.xgroupCreate(STREAM_NAME, GROUP_NAME, StreamEntryID.XGROUP_LAST_ENTRY, true);
        } catch (final Exception e) {
            // Group might exist already
            log.info("Group was not created: " + e);
        }

        final Instant start = new Instant();
        final AtomicBoolean consumerReady = new AtomicBoolean(false);

        final Thread consumer = new Thread(() -> consume(consumerReady));
        final Thread producer = new Thread(() -> produce(consumerReady));

        consumer.start();
        producer.start();

        while (consumer.isAlive() || producer.isAlive()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }

        final Duration duration = start.toDuration();
        log.info("Finished %s messages with %s after %s", NUMOFEVENTS,
                new ProcessedEventsRateString(NUMOFEVENTS, duration), duration);
    }

    @Override
    public void tearDown() throws Exception {
        REDIS.close();
    }

    private void produce(final AtomicBoolean consumerReady) {
        final Instant startOverall = new Instant();
        while (!consumerReady.get()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }

        final Instant startMessaging = new Instant();
        final XAddParams addParams = new XAddParams();

        for (long i = 0; i < NUMOFEVENTS; i++) {
            client.xadd(STREAM_NAME, addParams, Map.of("msg", MESSAGE + i));
        }

        final Duration durationOverall = startOverall.toDuration();
        final Duration durationMessaging = startMessaging.toDuration();
        log.info("producer finished after %s|%s with %s|%s messages", durationOverall, durationMessaging,
                new ProcessedEventsRateString(NUMOFEVENTS, durationOverall),
                new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging));
    }

    private void consume(final AtomicBoolean consumerReady) {
        final Instant startOverall = new Instant();
        Instant startMessaging = null;
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);

        consumerReady.set(true);

        long i = 0;
        final Map<String, StreamEntryID> streamQuery = Map.of(STREAM_NAME, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY);

        final XReadGroupParams readParams = new XReadGroupParams().count(32).block(200);

        while (i < NUMOFEVENTS) {
            final List<Entry<String, List<StreamEntry>>> msgs = client.xreadGroup(GROUP_NAME, CONSUMER_ID, readParams,
                    streamQuery);

            if (msgs == null || msgs.isEmpty()) {
                continue;
            }

            for (final Entry<String, List<StreamEntry>> streamResult : msgs) {
                for (final StreamEntry m : streamResult.getValue()) {
                    final String received = m.getFields().get("msg");

                    if (startMessaging == null) {
                        startMessaging = new Instant();
                    }

                    Assertions.checkEquals(MESSAGE + i, received);

                    client.xack(STREAM_NAME, GROUP_NAME, m.getID());
                    i++;

                    if (loopCheck.checkClockNoInterrupt()) {
                        final Duration durationMessaging = startMessaging.toDuration();
                        log.info("consumer received %s messages with %s since %s", i,
                                new ProcessedEventsRateString(i, durationMessaging), durationMessaging);
                    }

                    if (i >= NUMOFEVENTS) {
                        break;
                    }
                }
            }

        }

        final Duration durationOverall = startOverall.toDuration();
        final Duration durationMessaging = startMessaging != null ? startMessaging.toDuration() : Duration.ZERO;
        log.info("consumer finished after %s|%s with %s|%s messages", durationOverall, durationMessaging,
                new ProcessedEventsRateString(NUMOFEVENTS, durationOverall),
                new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging));

    }
}
