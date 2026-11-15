package de.invesdwin.context.integration.grid.docker;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.RemoveContainerCmd;
import com.github.dockerjava.api.command.StopContainerCmd;
import com.github.dockerjava.api.model.Container;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;

/**
 * The caller must never re-bind the fixed host ports of a single-owner slot while a labeled container still holds them:
 * a failed container start has to run the labeled reclaim (stop and remove, naming the container) BEFORE the next start
 * attempt, retry only a bounded number of times with a waiting gap, and a dead reaper handshake must not abort the
 * attempt - the reclaim is the single-owner guard then (measured 2026-11-05: 'Could not connect to Ryuk' and 'Timed out
 * waiting for container port to open' storms over a service that could never bind its ports).
 */
@NotThreadSafe
public class ContainerStarterTest extends ATest {

    private static final String LABEL = "de.invesdwin.context.integration.grid.docker.testOwner";

    private final List<String> callOrder = new ArrayList<>();
    private final List<String> claimedIds = new ArrayList<>();

    @Test
    public void firstStartSuccessClaimsNothing() {
        final AtomicInteger starts = new AtomicInteger();
        final DockerClient client = fakeClient(List.of());
        ContainerStarter.startWithSingleOwnerClaim(ContainerOwnership.of(LABEL), () -> starts.incrementAndGet(),
                () -> client, 3, noopSleep());
        Assertions.assertThat(starts.get()).isEqualTo(1);
        //nothing was reclaimed: not even a lookup, the first attempt just runs
        Assertions.assertThat(callOrder).isEmpty();
    }

    @Test
    public void failedStartReclaimsLabeledHolderBeforeNextStart() {
        final AtomicInteger starts = new AtomicInteger();
        final DockerClient client = fakeClient(List.of(labeledContainer("deadbeefcafe1234", "running")));
        ContainerStarter.startWithSingleOwnerClaim(ContainerOwnership.of(LABEL), () -> {
            callOrder.add("start");
            if (starts.incrementAndGet() == 1) {
                throw new ContainerLaunchException(
                        "Could not start container: Timed out waiting for container port to open (localhost ports: "
                                + "[4001, 4002] should be listening)");
            }
        }, () -> client, 3, noopSleep());
        //order is the whole point: the reclaim (list/stop/remove) must sit between the failed start and the retry, the
        //retry is what re-binds the fixed ports
        Assertions.assertThat(callOrder).containsExactly("start", "list", "stop", "remove", "start");
        //the holder is named by its docker short id
        Assertions.assertThat(claimedIds).containsExactly("deadbeefcafe");
    }

    @Test
    public void ryukHandshakeFailureEndsTheAttemptInsteadOfRetrying() {
        final AtomicInteger starts = new AtomicInteger();
        final DockerClient client = fakeClient(List.of());
        final IllegalStateException thrown = Assertions.catchThrowableOfType(() -> ContainerStarter
                .startWithSingleOwnerClaim(ContainerOwnership.of(LABEL), () -> {
                    callOrder.add("start");
                    starts.incrementAndGet();
                    throw new IllegalStateException("Could not connect to Ryuk at localhost:32799");
                }, () -> client, 3, noopSleep()), IllegalStateException.class);
        Assertions.assertThat(thrown).isNotNull();
        //testcontainers 2.0.3 caches the failed reaper handshake in its client factory (cachedClientFailure), so every
        //later docker call of this JVM rethrows it without another try (measured 2026-11-05: attempt 2 failed 1 ms
        //after attempt 1, attempt 3 after 0 ms). Retrying in this JVM can not reach the reaper again - the caller's
        //ladder is the outer bound, and it is what gets a fresh handshake with the next attempt's JVM state.
        Assertions.assertThat(starts.get()).isEqualTo(1);
        Assertions.assertThat(callOrder).containsExactly("start");
    }

    @Test
    public void attemptsAreBoundedAndLastFailurePropagates() {
        final AtomicInteger starts = new AtomicInteger();
        final DockerClient client = fakeClient(List.of());
        final ContainerLaunchException last = Assertions.catchThrowableOfType(() -> ContainerStarter
                .startWithSingleOwnerClaim(ContainerOwnership.of(LABEL), () -> {
                    callOrder.add("start");
                    starts.incrementAndGet();
                    throw new ContainerLaunchException("Could not start container");
                }, () -> client, 3, noopSleep()), ContainerLaunchException.class);
        Assertions.assertThat(last).isNotNull();
        Assertions.assertThat(starts.get()).isEqualTo(3);
        //between the three starts there are two claims and two waiting gaps
        Assertions.assertThat(callOrder).containsExactly("start", "list", "start", "list", "start");
    }

    private static LongConsumer noopSleep() {
        return millis -> {
            //no real waiting in the offline test
        };
    }

    private static Container labeledContainer(final String id, final String state) {
        final Container container = new Container() {
            @Override
            public String getId() {
                return id;
            }

            @Override
            public String getState() {
                return state;
            }
        };
        final Map<String, String> labels = ILockCollectionFactory.getInstance(false).newMap();
        labels.put(LABEL, LABEL);
        container.labels = labels;
        return container;
    }

    /**
     * A recording docker client: it only serves the commands the reclaimer uses and records their order, so the
     * reclaim-before-next-start order and the named container can be asserted without a docker daemon.
     */
    private DockerClient fakeClient(final List<Container> listed) {
        return (DockerClient) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { DockerClient.class }, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "listContainersCmd":
                            callOrder.add("list");
                            return cmdProxy(ListContainersCmd.class, listed);
                        case "stopContainerCmd":
                            callOrder.add("stop");
                            claimedIds.add(ContainerOwnership.shortId((String) args[0]));
                            return cmdProxy(StopContainerCmd.class, null);
                        case "removeContainerCmd":
                            callOrder.add("remove");
                            return cmdProxy(RemoveContainerCmd.class, null);
                        case "toString":
                            return "fakeClient";
                        case "hashCode":
                            return 0;
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    private static Object cmdProxy(final Class<?> cmdInterface, final Object execResult) {
        return Proxy.newProxyInstance(cmdInterface.getClassLoader(), new Class<?>[] { cmdInterface },
                (proxy, method, args) -> {
                    if ("exec".equals(method.getName())) {
                        return execResult;
                    }
                    return proxy;
                });
    }

}
