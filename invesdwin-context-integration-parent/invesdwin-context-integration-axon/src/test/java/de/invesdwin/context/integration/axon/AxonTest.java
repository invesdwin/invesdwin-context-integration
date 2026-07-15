package de.invesdwin.context.integration.axon;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.NotThreadSafe;

import org.awaitility.Awaitility;
import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.command.AxonServerCommandBus;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.axonframework.test.server.AxonServerContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.thoughtworks.xstream.XStream;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class AxonTest extends ATest {

    @Container
    private static final AxonServerContainer AXON = new AxonServerContainer().withAxonServerName("axonserver-test")
            .withAxonServerHostname("localhost");

    private static Configuration configuration;
    private static final List<PongEvent> RECEIVED = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void startAxon() {
        final AxonServerConfiguration axonServerConfiguration = AxonServerConfiguration.builder()
                .servers(AXON.getHost() + ":" + AXON.getGrpcPort())
                .build();

        final AxonServerConnectionManager connectionManager = AxonServerConnectionManager.builder()
                .axonServerConfiguration(axonServerConfiguration)
                .build();

        // FIX: Configure XStream security to allow your classes
        final XStream xStream = new XStream();
        xStream.allowTypesByWildcard(new String[] { "de.invesdwin.**", "org.axonframework.**", "java.**" });
        final Serializer serializer = XStreamSerializer.builder().xStream(xStream).build();

        final Configurer configurer = DefaultConfigurer.defaultConfiguration()
                .configureSerializer(c -> serializer)
                .configureMessageSerializer(c -> serializer)
                .configureEventSerializer(c -> serializer)

                .registerComponent(AxonServerConfiguration.class, c -> axonServerConfiguration)
                .registerComponent(AxonServerConnectionManager.class, c -> connectionManager)
                .configureEventStore(c -> AxonServerEventStore.builder()
                        .configuration(axonServerConfiguration)
                        .platformConnectionManager(connectionManager)
                        .snapshotFilter(c.snapshotFilter())
                        .eventSerializer(c.serializer())
                        .snapshotSerializer(c.serializer())
                        .build())
                .configureCommandBus(c -> AxonServerCommandBus.builder()
                        .axonServerConnectionManager(connectionManager)
                        .localSegment(SimpleCommandBus.builder().build())
                        .configuration(axonServerConfiguration)
                        .serializer(c.serializer()) // This will now fetch our custom secure serializer
                        .routingStrategy(AnnotationRoutingStrategy.defaultStrategy())
                        .build())
                .configureAggregate(PingAggregate.class)
                .eventProcessing(ep -> ep.registerEventHandler(c -> new PongListener()));

        configuration = configurer.buildConfiguration();
        configuration.start();
    }

    @AfterAll
    static void stopAxon() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void pingProducesPongThroughRealAxonServer() {
        final CommandGateway commandGateway = configuration.commandGateway();
        commandGateway.sendAndWait(new PingCommand("agg-1"));

        Awaitility.await()
                .atMost(Duration.TEN_SECONDS.javaTimeValue())
                .untilAsserted(
                        () -> Assertions.assertThat(RECEIVED).extracting(PongEvent::getId).containsExactly("agg-1"));
    }

    // ---- Messages ----

    public static class PingCommand {
        @AggregateIdentifier
        private final String id;

        public PingCommand(final String id) {
            this.id = id;
        }
    }

    public static class PongEvent {
        private String id;

        public PongEvent() {} // needed for JSON (de)serialization over gRPC

        public PongEvent(final String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    public static class PingAggregate {

        @AggregateIdentifier
        private String id;

        public PingAggregate() {}

        @CommandHandler
        public PingAggregate(final PingCommand command) {
            AggregateLifecycle.apply(new PongEvent(command.id));
        }

        @EventSourcingHandler
        public void on(final PongEvent event) {
            this.id = event.id;
        }
    }

    public static class PongListener {
        @EventHandler
        public void on(final PongEvent event) {
            RECEIVED.add(event);
        }
    }

}