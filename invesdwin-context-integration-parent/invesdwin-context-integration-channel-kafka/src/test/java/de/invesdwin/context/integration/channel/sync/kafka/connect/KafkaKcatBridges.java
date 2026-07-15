package de.invesdwin.context.integration.channel.sync.kafka.connect;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaBridges;
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.log.Log;

@NotThreadSafe
public class KafkaKcatBridges implements IKafkaBridges {
    private final Log log = new Log(this);
    private final IKafkaContainer<?> kafkaContainer;
    private final List<GenericContainer<?>> bridges = new ArrayList<>();

    public KafkaKcatBridges(final IKafkaContainer<?> kafkaContainer) {
        this.kafkaContainer = kafkaContainer;
    }

    //CHECKSTYLE:OFF
    @Override
    public IKafkaBridges withNetwork(final Network network) {
        return this;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
        for (final GenericContainer<?> bridge : bridges) {
            try {
                bridge.stop();
            } catch (final Exception ignore) {
            }
        }
        bridges.clear();
    }

    @Override
    public GenericContainer<?> startBridge(final String inputTopic, final String outputTopic) {

        // making kcat wait to read up to 5 times the message count, as not setting it,
        //or setting it to message count alone would make kcat close early making test fail
        final String cmd = String.format(
                "python /kpipe/pipe.py --server %s --source-topic %s --sink-topic %s --from-beginning -n %d",
                kafkaContainer.getBootstrapServers(), inputTopic, outputTopic, AChannelTest.MESSAGE_COUNT * 5);

        final GenericContainer<?> bridgeContainer = new GenericContainer<>("cmantas/kpipe").withNetworkMode("host")
                .withCreateContainerCmdModifier(c -> c.withEntrypoint("sh"))
                .withCommand("-lc", cmd);
        bridgeContainer.start();
        bridges.add(bridgeContainer);
        return bridgeContainer;
    }

}
