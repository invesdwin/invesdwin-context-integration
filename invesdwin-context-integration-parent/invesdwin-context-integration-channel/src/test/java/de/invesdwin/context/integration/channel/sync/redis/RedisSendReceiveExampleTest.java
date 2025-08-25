package de.invesdwin.context.integration.channel.sync.redis;

import java.util.List;
import java.util.Map;
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
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;

@Testcontainers
@NotThreadSafe
public class RedisSendReceiveExampleTest extends ATest {

    private static final String STREAM_NAME = "helloworldtopic";
    private static final String GROUP_NAME = "test_consumer_group";
    private static final String CONSUMER_ID = "c1";
    private static final String MESSAGE = "helloworldtest";
    private static final int NUMOFEVENTS = AChannelTest.MESSAGE_COUNT;

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    private final RedisClient client = RedisClient.create(getRedisUri());//creates and connects client to uri
    private final StatefulRedisConnection<String, String> conn = client.connect();//Open a new connection to a Redis server that treats keys and values as UTF-8 strings.
    private final RedisStreamCommands<String, String> stream = conn.sync();

    private String getRedisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @Test
    public void test() throws InterruptedException {

        final RedisCommands<String, String> cmd = conn.sync();//returns commands to use for the current connection

        //checks connection is good by pinging server and then checking K/V pairs
        Assertions.checkEquals("PONG", cmd.ping());
        cmd.set("hello", "world");
        Assertions.checkEquals("world", cmd.get("hello"));

        final RedisStreamCommands<String, String> stream = conn.sync();
        stream.xgroupCreate(XReadArgs.StreamOffset.from(STREAM_NAME, "0-0"), GROUP_NAME,
                XGroupCreateArgs.Builder.mkstream());

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

    private void produce(final AtomicBoolean consumerReady) {
        final Instant startOverall = new Instant();
        while (!consumerReady.get()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }

        final Instant startMessaging = new Instant();

        for (int i = 0; i < NUMOFEVENTS; i++) {
            stream.xadd(STREAM_NAME, Map.of("msg", MESSAGE + i));
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

        int i = 0;
        while (i < NUMOFEVENTS) {
            final List<StreamMessage<String, String>> msgs = stream.xreadgroup(Consumer.from(GROUP_NAME, CONSUMER_ID),
                    XReadArgs.Builder.block(200).count(32), XReadArgs.StreamOffset.lastConsumed(STREAM_NAME));

            if (msgs == null || msgs.isEmpty()) {
                continue;
            }

            for (final StreamMessage<String, String> m : msgs) {
                final String received = m.getBody().get("msg");
                if (startMessaging == null) {
                    startMessaging = new Instant();
                }
                Assertions.checkEquals(MESSAGE + i, received);
                // acknowledge
                stream.xack(STREAM_NAME, GROUP_NAME, m.getId());
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
        final Duration durationOverall = startOverall.toDuration();
        final Duration durationMessaging = startMessaging != null ? startMessaging.toDuration() : Duration.ZERO;
        log.info("consumer finished after %s|%s with %s|%s messages", durationOverall, durationMessaging,
                new ProcessedEventsRateString(NUMOFEVENTS, durationOverall),
                new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging));

    }
}
