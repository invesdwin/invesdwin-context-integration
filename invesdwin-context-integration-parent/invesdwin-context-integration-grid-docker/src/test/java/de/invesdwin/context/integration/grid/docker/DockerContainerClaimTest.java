package de.invesdwin.context.integration.grid.docker;

import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * The claim that the offline cells in {@link DockerContainerTestSuite} pin against a recording docker client, run against
 * a real daemon: that the stamp reaches the docker host so a JVM that never started the container can still find it, that
 * a labeled holder is really removed, that an orphan no reaper ever owns is reclaimed the same way, and that a foreign
 * container survives the sweep.
 * <p>
 * The occupancy proof is the daemon's own refusal, not a connection attempt: while a holder is up, a second container
 * asking for the same host publish is refused at start ("port is already allocated" on the daemon measured here), and the
 * same bind is accepted once the reclaim has taken the holder. So the fixture needs no service in it — it logs one line
 * and sleeps, which keeps the cells free of any protocol surface on the host. (A listening fixture would have needed the
 * image to carry a server: the stock JDK image's {@code jwebserver} is not shipped in it, and {@code java -m
 * jdk.httpserver} only works from the JDK that gave that module a main class, one version newer than this fixture's.)
 * <p>
 * Every cell is self-contained: its own free host port, its own fixture, removal in a finally block. The fixture image is
 * a general-purpose JDK image, pulled on first use on a box that does not have it yet — that pull is the price of
 * depending on nothing installed on the host.
 * <p>
 * A box without a usable daemon SKIPS rather than passes; the reason names which case it is. The gate is inside the
 * cells, never on the suite class, because a Jupiter condition on a suite does not gate the suite engine.
 */
@NotThreadSafe
public class DockerContainerClaimTest extends ATest {

    /**
     * Scoped to this JVM: the docker daemon is shared machine state, and a reclaim sweeps by label, so two builds running
     * the same suite against one daemon would otherwise take each other's fixtures mid-cell. A per-JVM random UUID is the
     * nonce — this module compiles at source level 8, so the JDK 9 pid accessor is not available here.
     */
    private static final String LABEL = "de.invesdwin.context.integration.grid.docker.claimfixture."
            + UUID.randomUUID();

    /**
     * A general-purpose image that is on every box's registry cache path and needs no build of our own; the fixture only
     * has to stay running, so its contents are irrelevant beyond {@code sh}.
     */
    private static final DockerImageName FIXTURE_IMAGE = DockerImageName.parse("eclipse-temurin:17-jdk");
    private static final String FIXTURE_READY_MARKER = "claim-fixture-up";
    private static final String[] FIXTURE_COMMAND = { "sh", "-c",
            "echo " + FIXTURE_READY_MARKER + "; exec sleep infinity" };
    private static final int FIXTURE_CONTAINER_PORT = 18080;

    @Test
    public void stampReachesTheDaemon() {
        assumeDockerDaemon();
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        final int hostPort = freeHostPort();
        final GenericContainer<?> holder = newFixture(hostPort, ownership);
        try {
            holder.start();
            //what a reclaim sees from a JVM that never started this container: the stamp has to live on the docker host,
            //not only in the object graph that created it
            final List<Container> listed = listByLabel(ownership);
            Assertions.assertThat(listed).hasSize(1);
            Assertions.assertThat(ownership.owns(listed.get(0).getLabels())).isTrue();
            Assertions.assertThat(listed.get(0).getId()).isEqualTo(holder.getContainerId());
        } finally {
            removeQuietly(holder.getContainerId());
        }
    }

    @Test
    public void reclaimFreesTheFixedPortForTheNextOwner() {
        assumeDockerDaemon();
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        final int hostPort = freeHostPort();
        final GenericContainer<?> holder = newFixture(hostPort, ownership);
        try {
            holder.start();
            //(1) the port really is held: the daemon refuses a second container on the same publish, so the success
            //asserted in (3) below says something about the reclaim and not about docker being permissive
            Assertions.assertThat(bindRefusedWhileHolderIsUp(hostPort)).isTrue();
            Assertions.assertThat(listByLabel(ownership)).hasSize(1);

            ContainerReclaimer.reclaimPreExistingContainers(ownership, client(), "integration test");
            Assertions.assertThat(listByLabel(ownership)).isEmpty();

            //(2) the claim the mechanism exists for: the bind that was just refused now lands, with one owner left
            final GenericContainer<?> successor = newFixture(hostPort, ownership);
            try {
                successor.start();
                Assertions.assertThat(listByLabel(ownership)).hasSize(1);
                Assertions.assertThat(listByLabel(ownership).get(0).getId())
                        .isEqualTo(successor.getContainerId());
            } finally {
                removeQuietly(successor.getContainerId());
            }
        } finally {
            removeQuietly(holder.getContainerId());
        }
    }

    @Test
    public void orphanWithNoReaperSessionIsReclaimedBeforeTheNextBind() {
        assumeDockerDaemon();
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        final int hostPort = freeHostPort();
        //created straight through the docker API, so no testcontainers session ever owned it — the crash-orphan case
        //the reaper cannot cover by construction (it filters on its own session id)
        final DockerClient client = client();
        CreateContainerResponse created = null;
        try {
            created = client.createContainerCmd(FIXTURE_IMAGE.toString())
                    .withCmd(FIXTURE_COMMAND)
                    //the same map the reclaim looks up with: key and value both the service name
                    .withLabels(ownership.newLabelFilter())
                    .withHostConfig(HostConfig.newHostConfig()
                            .withPortBindings(PortBinding.parse(hostPort + ":" + FIXTURE_CONTAINER_PORT)))
                    .exec();
            client.startContainerCmd(created.getId()).exec();
            Assertions.assertThat(listByLabel(ownership)).hasSize(1);

            ContainerReclaimer.reclaimPreExistingContainers(ownership, client, "integration test");
            Assertions.assertThat(listByLabel(ownership)).isEmpty();
            Assertions.assertThat(exists(created.getId())).isFalse();

            final GenericContainer<?> successor = newFixture(hostPort, ownership);
            try {
                successor.start();
                Assertions.assertThat(listByLabel(ownership)).hasSize(1);
            } finally {
                removeQuietly(successor.getContainerId());
            }
        } finally {
            if (created != null) {
                removeQuietly(created.getId());
            }
        }
    }

    @Test
    public void foreignHoldersAreNeverTouched() {
        assumeDockerDaemon();
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        //an unlabelled container on a port of its own…
        final int unlabelledPort = freeHostPort();
        final GenericContainer<?> unlabelled = newFixture(unlabelledPort, null);
        //…and the dangerous shape: OUR key carrying someone else's value, which the exact-match filter excludes at the
        //daemon and which the local owns() guard also refuses. Both lines of defence are asserted here as one outcome:
        //a sweep for our ownership can not take a container that is not ours.
        final int foreignValuePort = freeHostPort();
        final GenericContainer<?> foreignValue = newFixtureWithForeignValue(foreignValuePort);
        try {
            unlabelled.start();
            foreignValue.start();

            ContainerReclaimer.reclaimPreExistingContainers(ownership, client(), "integration test");

            Assertions.assertThat(exists(unlabelled.getContainerId())).isTrue();
            Assertions.assertThat(exists(foreignValue.getContainerId())).isTrue();
            Assertions.assertThat(listByLabel(ownership)).isEmpty();
        } finally {
            removeQuietly(unlabelled.getContainerId());
            removeQuietly(foreignValue.getContainerId());
        }
    }

    /**
     * Asks the daemon for a second container on the same host publish while the caller's holder is up, and reports whether
     * it was refused. The refusal arrives at start rather than at create, and the container the daemon left behind in the
     * created state is removed here, not by the caller.
     */
    private static boolean bindRefusedWhileHolderIsUp(final int hostPort) {
        final DockerClient client = client();
        final CreateContainerResponse attempt = client.createContainerCmd(FIXTURE_IMAGE.toString())
                .withCmd(FIXTURE_COMMAND)
                .withHostConfig(HostConfig.newHostConfig()
                        .withPortBindings(PortBinding.parse(hostPort + ":" + FIXTURE_CONTAINER_PORT)))
                .exec();
        try {
            client.startContainerCmd(attempt.getId()).exec();
            return false;
        } catch (final RuntimeException e) {
            return true;
        } finally {
            removeQuietly(attempt.getId());
        }
    }

    @SuppressWarnings("deprecation")
    private static GenericContainer<?> newFixture(final int hostPort, final ContainerOwnership ownership) {
        final FixedHostPortGenericContainer<?> container = newFixedHostPortFixture(hostPort);
        if (ownership != null) {
            ownership.applyTo(container);
        }
        return container;
    }

    @SuppressWarnings("deprecation")
    private static GenericContainer<?> newFixtureWithForeignValue(final int hostPort) {
        final FixedHostPortGenericContainer<?> container = newFixedHostPortFixture(hostPort);
        container.withLabel(LABEL, "some.other.owner");
        return container;
    }

    @SuppressWarnings("deprecation")
    private static FixedHostPortGenericContainer<?> newFixedHostPortFixture(final int hostPort) {
        final FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(
                FIXTURE_IMAGE.toString());
        container.withFixedExposedPort(hostPort, FIXTURE_CONTAINER_PORT);
        container.withCommand(FIXTURE_COMMAND);
        //the fixture serves nothing, so readiness is its own log line — the default wait would look for a listener
        container.waitingFor(Wait.forLogMessage(".*" + FIXTURE_READY_MARKER + ".*", 1));
        return container;
    }

    private static void assumeDockerDaemon() {
        final boolean usable;
        try {
            usable = client().versionCmd().exec() != null;
        } catch (final RuntimeException e) {
            Assumptions.assumeTrue(false,
                    "docker client unusable on this box, so the real-daemon cells skip: "
                            + e.getClass().getSimpleName() + ": " + e.getMessage());
            return;
        }
        Assumptions.assumeTrue(usable,
                "no usable docker daemon on this box — these cells skip, they do not pass; the claim itself is covered "
                        + "offline by " + DockerContainerTestSuite.class.getSimpleName());
    }

    private static DockerClient client() {
        return DockerClientFactory.lazyClient();
    }

    /**
     * A port the OS hands us, held open for the length of one call. There is a race in reusing that number for a docker
     * publish, and if something takes it in between the cell fails loudly on the bind rather than passing quietly.
     */
    private static int freeHostPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (final Exception e) {
            throw new IllegalStateException("no free host port for the container fixture", e);
        }
    }

    private static List<Container> listByLabel(final ContainerOwnership ownership) {
        return client().listContainersCmd()
                .withShowAll(true)
                .withLabelFilter(ownership.newLabelFilter())
                .exec();
    }

    private static boolean exists(final String containerId) {
        return !client().listContainersCmd()
                .withShowAll(true)
                .withIdFilter(List.of(containerId))
                .exec()
                .isEmpty();
    }

    private static void removeQuietly(final String containerId) {
        if (containerId == null) {
            return;
        }
        try {
            client().removeContainerCmd(containerId).withForce(true).exec();
        } catch (final Exception e) {
            //a container the reaper already took, or one that never came up: a cleanup failure must not be reported as
            //the cell's failure, since the cell's own assertions have already run
        }
    }

}
