package de.invesdwin.context.integration.grid.docker;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.RemoveContainerCmd;
import com.github.dockerjava.api.command.StopContainerCmd;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.math.Integers;

/**
 * The reclaim must only ever claim containers carrying our own service label - a wrong lookup must never touch a foreign
 * container - and must only issue stop for containers that actually run.
 * <p>
 * It must also survive the takeover race: the previous session's testcontainers reaper reaps the orphan concurrently
 * (measured 2026-11-05: the reaper killed a kill -9 orphan about a second after its client died, while our stop was
 * still in flight), so a stop or remove that finds the container gone or mid-removal is not a failure - the claim
 * re-checks until no labeled holder of the fixed ports is left. And a docker client that can not be acquired
 * (testcontainers 2.0.3 caches a failed reaper handshake for the whole JVM) must not abort the caller's attempt: the
 * claim is the single-owner guard, so it has to be the part that stays standing.
 */
@NotThreadSafe
public class ContainerReclaimerTest extends ATest {

    private static final String LABEL = "de.invesdwin.context.integration.grid.docker.testOwner";

    private final List<String> callOrder = new ArrayList<>();

    @Test
    public void onlyOwnLabeledContainersAreSelected() {
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        final List<Container> listed = new ArrayList<>();
        //unlabeled foreign container
        listed.add(newContainer(null));
        //labeled foreign container without our key
        listed.add(newContainer(labeled("de.invesdwin.some.other.owner", LABEL)));
        //our key with a foreign value is not ours either
        listed.add(newContainer(labeled(LABEL, "someone-else")));
        //ours
        final Container own = newContainer(labeled(LABEL, LABEL));
        listed.add(own);

        final List<Container> owned = ownership.selectOwned(listed);
        Assertions.assertThat(owned.size()).isEqualTo(1);
        Assertions.assertThat(owned.get(0)).isSameAs(own);
    }

    @Test
    public void onlyRunningContainersAreStopped() {
        Assertions.assertThat(ContainerReclaimer.isRunning("running")).isTrue();
        Assertions.assertThat(ContainerReclaimer.isRunning("exited")).isFalse();
        Assertions.assertThat(ContainerReclaimer.isRunning("created")).isFalse();
        Assertions.assertThat(ContainerReclaimer.isRunning(null)).isFalse();
    }

    @Test
    public void goneDuringStopIsStillClaimed() {
        //the reaper of the old session may remove the orphan while our stop is in flight
        final DockerClient client = fakeClient(List.of(List.of(labeledContainer("deadbeefcafe1234", "running"))),
                List.of(), List.of(new NotFoundException("no such container")));
        ContainerReclaimer.reclaimPreExistingContainers(ContainerOwnership.of(LABEL), client, "startup");
        //stop failed with gone, remove still claims the slot; no second sweep was needed
        Assertions.assertThat(callOrder).containsExactly("list", "stop", "remove");
    }

    @Test
    public void removalRaceIsRetriedUntilTheHolderIsGone() {
        //first sweep: docker answers 'removal of container ... is already in progress' (concurrent reaper removal)
        //second sweep: the same container is listed again and this time the remove goes through
        final ConflictException inProgress = new ConflictException("removal of container abc is already in progress");
        final DockerClient client = fakeClient(
                List.of(List.of(labeledContainer("deadbeefcafe1234", "exited")),
                        List.of(labeledContainer("deadbeefcafe1234", "exited")),
                        List.of()),
                List.of(inProgress), List.of());
        ContainerReclaimer.reclaimPreExistingContainers(ContainerOwnership.of(LABEL), client, "start failure 1", true);
        //the claim re-checks by label instead of accepting a holder that still owns the fixed ports
        Assertions.assertThat(callOrder).containsExactly("list", "remove", "list", "remove");
    }

    @Test
    public void dockerClientFailureDoesNotAbortTheClaim() {
        //testcontainers 2.0.3 caches a failed reaper handshake in its client factory: every later docker call in this
        //JVM rethrows it, so the claim must degrade to a warning instead of throwing the caller's attempt down
        ContainerReclaimer.reclaimPreExistingContainers(ContainerOwnership.of(LABEL), () -> {
            throw new IllegalStateException("Could not connect to Ryuk at localhost:32799");
        }, "startup");
        Assertions.assertThat(callOrder).isEmpty();
    }

    private static Map<String, String> labeled(final String key, final String value) {
        final Map<String, String> labels = ILockCollectionFactory.getInstance(false).newMap();
        labels.put(key, value);
        return labels;
    }

    private static Container newContainer(final Map<String, String> labels) {
        final Container container = new Container();
        container.labels = labels;
        return container;
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
        container.labels = labeled(LABEL, LABEL);
        return container;
    }

    /**
     * A recording docker client serving only the commands the reclaimer uses: each list call returns the next scripted
     * listing, each remove call the next scripted failure, so the sweep order and the race handling are assertable
     * without a docker daemon.
     */
    private DockerClient fakeClient(final List<List<Container>> listings,
            final List<RuntimeException> removeFailures,
            final List<RuntimeException> stopFailures) {
        final AtomicInteger listCalls = new AtomicInteger();
        final AtomicInteger removeCalls = new AtomicInteger();
        final AtomicInteger stopCalls = new AtomicInteger();
        return (DockerClient) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { DockerClient.class }, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "listContainersCmd":
                            callOrder.add("list");
                            return cmdProxy(ListContainersCmd.class,
                                    listings.get(Integers.min(listCalls.getAndIncrement(), listings.size() - 1)),
                                    null);
                        case "stopContainerCmd":
                            callOrder.add("stop");
                            return cmdProxy(StopContainerCmd.class, null, nextFailure(stopFailures, stopCalls));
                        case "removeContainerCmd":
                            callOrder.add("remove");
                            return cmdProxy(RemoveContainerCmd.class, null, nextFailure(removeFailures, removeCalls));
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

    private static RuntimeException nextFailure(final List<RuntimeException> failures, final AtomicInteger calls) {
        final int index = calls.getAndIncrement();
        return index < failures.size() ? failures.get(index) : null;
    }

    private static Object cmdProxy(final Class<?> cmdInterface, final Object execResult,
            final RuntimeException failure) {
        return Proxy.newProxyInstance(cmdInterface.getClassLoader(), new Class<?>[] { cmdInterface },
                (proxy, method, args) -> {
                    if ("exec".equals(method.getName())) {
                        if (failure != null) {
                            throw failure;
                        }
                        return execResult;
                    }
                    return proxy;
                });
    }

}
