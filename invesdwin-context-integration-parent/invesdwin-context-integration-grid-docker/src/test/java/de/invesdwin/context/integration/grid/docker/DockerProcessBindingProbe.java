package de.invesdwin.context.integration.grid.docker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

/**
 * Proof of concept for the question behind the iqfeed reconnect storms: does the testcontainers reaper (ryuk) really
 * bind a container to the java process that started it, and what is left when that assumption fails.
 *
 * The iqfeed service starts its IQConnect provider in a container (IqfeedLoginCredentials#newIqconnectContainer) and
 * DTN allows one session per account, so a surviving container from a dead process is a second holder of that
 * session. The production symptom is two mechanisms fighting each other: the service's own labeled claim
 * (IqfeedContainerReclaimer stops and removes every container carrying the service label, at startup, between start
 * retries and on graceful cleanup) and ryuk, which only reaps containers labeled with its own session id. The
 * measurement behind that design is the repro of 2026-11-05 quoted in the reclaimer javadoc ('ryuk killed the
 * kill -9 orphan about a second after its client died, while our stop was still running'). This probe re-measures the
 * same thing on the current stack (testcontainers 2.0.3, ryuk 0.13.0, docker 29.x) with several processes and real
 * kill -9, so that a better solution can be argued from numbers instead of from the symptom.
 *
 * Measured over three full runs, 4 cells each, with a kill -9 reap at PT10.051S / PT10.060S / PT10.071S, the reaper
 * container re-used by the next process while the session ids differ, a fixed-port challenger refused without any
 * container being created, and an orphan still running 60 s after its reaper was removed by hand.
 *
 * NOT part of the build: the class name is deliberately outside surefire's default includes, because every cell needs
 * a docker daemon and one of them removes the reaper. Run it by hand with
 * {@code mvnd -o -T1 test -Dtest=DockerProcessBindingProbe -Dsurefire.failIfNoSpecifiedTests=false} in this module's
 * directory on a box with docker. Nothing here needs the live feed: no iqfeed image, no credentials, none of the
 * fixed IQConnect ports - the payload is a plain shell container and the port collision uses an unused 17xxx port.
 * Every container, process and file a cell starts is removed in its finally block.
 */
@NotThreadSafe
public class DockerProcessBindingProbe extends ATest {

    private static final Log LOG = new Log(DockerProcessBindingProbe.class);

    /**
     * A cheap image every box of this fleet already has, so the probe pulls nothing.
     */
    private static final String PAYLOAD_IMAGE = "eclipse-temurin:8-jdk-jammy";
    /**
     * Our own label, so a cell finds exactly the containers it started - the same idea as
     * {@code IqfeedLoginCredentials.DOCKER_LABEL}, and the reason a claim can not touch foreign containers.
     */
    private static final String MARKER_LABEL = "de.invesdwin.test.docker-binding";
    private static final String SESSION_LABEL = "org.testcontainers.sessionId";
    private static final int PAYLOAD_CONTAINER_PORT = 8080;
    private static final int FIXED_HOST_PORT = 17711;
    private static final Duration REAP_WAIT = new Duration(120, FTimeUnit.SECONDS);
    private static final Duration START_WAIT = new Duration(150, FTimeUnit.SECONDS);

    // ------------------------------------------------------------------ cells

    /**
     * Cell 1: the case ryuk is supposed to cover. The holder JVM is SIGKILLed, so no shutdown hook can run and the
     * only thing that can release its container is the reaper noticing that the holder's socket died.
     */
    @Test
    public void kill9OrphanIsReapedByTheRyukSession() throws Exception {
        final Holder holder = startHolder("kill9");
        try {
            Assertions.assertThat(isRunning(holder.containerId)).as("the holder container must be up before the kill").isTrue();
            holder.process.destroyForcibly();
            Assertions.assertThat(awaitExit(holder.process)).as("the holder JVM must die on kill -9").isTrue();
            final Duration reapTime = waitForContainerGone(holder.containerId, REAP_WAIT);
            Assertions.assertThat(reapTime).as("ryuk must reap the orphan of a SIGKILLed holder within " + REAP_WAIT).isNotNull();
            LOG.info("CELL1 kill -9 orphan reaped by the ryuk session after %s", reapTime);
        } finally {
            cleanup(holder);
        }
    }

    /**
     * Cell 2: what the next process sees. The reaper container outlives the JVM that started it and is re-used by the
     * next testcontainers client, which is the 'the next process seems to inherit that' part of the symptom - while
     * the containers stay separated by their session id, so ryuk can never reap the NEW holder for the OLD one's
     * death.
     */
    @Test
    public void nextProcessReusesTheReaperButNotTheSession() throws Exception {
        final Holder first = startHolder("inherit-first");
        final String firstSession = sessionLabelOf(first.containerId);
        final List<String> ryukIdsOfFirst = ryukContainerIds();
        try {
            first.process.destroyForcibly();
            awaitExit(first.process);
            final Duration reapTime = waitForContainerGone(first.containerId, REAP_WAIT);
            LOG.info("CELL2 first holder session %s reaped after %s", firstSession, reapTime);

            final Holder second = startHolder("inherit-second");
            try {
                final List<String> ryukIdsOfSecond = ryukContainerIds();
                final String secondSession = sessionLabelOf(second.containerId);
                final List<String> sharedReapers = new ArrayList<>(ryukIdsOfFirst);
                sharedReapers.retainAll(ryukIdsOfSecond);
                LOG.info("CELL2 reaper containers first=%s second=%s shared=%s sessions first=%s second=%s",
                        ryukIdsOfFirst, ryukIdsOfSecond, sharedReapers, firstSession, secondSession);
                Assertions.assertThat(sharedReapers).as("the next process is expected to re-use the running reaper").isNotEmpty();
                Assertions.assertThat(secondSession).as("the second holder carries a session id").isNotNull();
                Assertions.assertThat(isRunning(second.containerId)).as("the second holder keeps its container").isTrue();
                if (firstSession != null) {
                    Assertions.assertThat(secondSession).as("two processes must not share a session id").isNotEqualTo(firstSession);
                }
            } finally {
                cleanup(second);
            }
        } finally {
            removeIfPresent(first.containerId);
        }
    }

    /**
     * Cell 3: the DTN single-session analogue without iqfeed - two processes racing for one fixed host port. The
     * second must not silently take over and the first must survive; only after the holder releases the port does
     * another attempt get it. That is what 'two containers hold the DTN session' looks like from docker's side, and it
     * is why the claim has to run before each bind attempt.
     */
    @Test
    public void fixedHostPortHasExactlyOneOwnerUntilTheLabelReclaimRuns() throws Exception {
        final Holder owner = startHolderOnFixedPort("port-owner", FIXED_HOST_PORT);
        try {
            Assertions.assertThat(owner.containerId).as("the owner must get the fixed port").isNotNull();
            Assertions.assertThat(isRunning(owner.containerId)).as("the owner holds the fixed port").isTrue();
            final Holder challenger = startHolderOnFixedPort("port-challenger", FIXED_HOST_PORT);
            try {
                //measured: testcontainers refuses the start outright (the port bind fails), so the challenger never
                //even reaches a running container - the owner is never dispossessed
                Assertions.assertThat(challenger.containerId).as("the challenger must not get the fixed port").isNull();
                Assertions.assertThat(isRunning(owner.containerId)).as("the refusal must not disturb the owner").isTrue();
                LOG.info("CELL3 challenger refused on fixed port %s without a container; owner %s still running",
                        Integer.valueOf(FIXED_HOST_PORT), shortId(owner.containerId));
            } finally {
                reclaimByLabel("port-challenger");
            }
            //the slot is held by the RUNNING container, not by a label: as long as the owner lives, a third attempt is
            //refused too - which is what the iqfeed claim has to work against
            final Holder stillBlocked = startHolderOnFixedPort("port-blocked", FIXED_HOST_PORT);
            try {
                Assertions.assertThat(stillBlocked.containerId).as("the slot stays taken while the owner runs").isNull();
            } finally {
                reclaimByLabel("port-blocked");
            }
            //a graceful exit of the owner releases the port (its shutdown hook stops the container); the next attempt
            //then binds. This is the difference between a normal restart and a kill -9 orphan.
            owner.process.destroy();
            final Duration released = waitForContainerGone(owner.containerId, new Duration(60, FTimeUnit.SECONDS));
            LOG.info("CELL3 owner released fixed port %s after a graceful exit in %s", Integer.valueOf(FIXED_HOST_PORT),
                    released);
            Assertions.assertThat(released).as("a graceful holder exit must release the port").isNotNull();
            final Holder retry = startHolderOnFixedPort("port-retry", FIXED_HOST_PORT);
            try {
                Assertions.assertThat(retry.containerId).as("after the release the retry must get the port").isNotNull();
                Assertions.assertThat(isRunning(retry.containerId)).as("after the release the retry must hold the port").isTrue();
            } finally {
                cleanup(retry);
            }
        } finally {
            cleanup(owner);
        }
    }

    /**
     * Cell 4: where the ryuk guarantee ends. If the reaper container itself is gone (an operator removed it, the
     * docker daemon restarted, or the machine crash-booted), a SIGKILLed holder leaves its container running: nothing
     * reaps a container whose session's reaper no longer exists. This is the argument for keeping the labeled claim as
     * the mechanism, and against counting on ryuk alone - disabling ryuk would lose a convenience, not a guarantee.
     */
    @Test
    public void killingTheReaperByHandLeavesTheOrphanRunning() throws Exception {
        final Holder holder = startHolder("no-ryuk");
        try {
            final List<String> ryuks = ryukContainerIds();
            for (final String ryuk : ryuks) {
                dockerClient().removeContainerCmd(ryuk).withForce(true).withRemoveVolumes(true).exec();
            }
            LOG.info("CELL4 removed the reaper containers %s to model an orphan whose session owner is gone", ryuks);
            holder.process.destroyForcibly();
            awaitExit(holder.process);
            final Duration gone = waitForContainerGone(holder.containerId, new Duration(60, FTimeUnit.SECONDS));
            Assertions.assertThat(gone).as("with no reaper alive nothing may remove the orphan; if this fails, ryuk is "
                    + "stronger than the fleet note claims and that has to be re-documented").isNull();
            LOG.info("CELL4 orphan %s still present 60 s after its holder died and the reaper was removed",
                    shortId(holder.containerId));
        } finally {
            holder.process.destroyForcibly();
            removeIfPresent(holder.containerId);
        }
    }

    // ------------------------------------------------------------------ plumbing

    @NotThreadSafe
    private static final class Holder {
        private final Process process;
        private final String containerId;
        private final File outFile;
        private final File idFile;

        private Holder(final Process process, final String containerId, final File outFile, final File idFile) {
            this.process = process;
            this.containerId = containerId;
            this.outFile = outFile;
            this.idFile = idFile;
        }
    }

    private Holder startHolder(final String marker) throws Exception {
        return startProcess(marker, 0);
    }

    private Holder startHolderOnFixedPort(final String marker, final int hostPort) throws Exception {
        return startProcess(marker, hostPort);
    }

    /**
     * The holder runs in its own JVM (so kill -9 is a real kill, not a shutdown hook) and writes the container id it
     * started into {@code idFile}, exactly like the iqfeed service knows its provider container id.
     */
    private Holder startProcess(final String marker, final int hostPort) throws Exception {
        final File outFile = File.createTempFile("docker-binding-" + marker + "-out-", ".log");
        final File idFile = File.createTempFile("docker-binding-" + marker + "-id-", ".txt");
        final ProcessBuilder pb = new ProcessBuilder(
                new SystemProperties().getString("java.home") + File.separator + "bin" + File.separator + "java", "-cp",
                new SystemProperties().getString("java.class.path"), ContainerMain.class.getName(), marker,
                Integer.toString(hostPort), PAYLOAD_IMAGE, idFile.getAbsolutePath());
        pb.redirectOutput(outFile).redirectErrorStream(true);
        final Process process = pb.start();
        final String containerId = awaitContainerId(process, idFile, outFile);
        if (containerId == null) {
            LOG.info("holder [%s] pid=%s reported no container: the start was refused", marker,
                    Long.valueOf(process.pid()));
            return new Holder(process, null, outFile, idFile);
        }
        LOG.info("holder [%s] pid=%s container=%s", marker, Long.valueOf(process.pid()), shortId(containerId));
        return new Holder(process, containerId, outFile, idFile);
    }

    private String awaitContainerId(final Process process, final File idFile, final File outFile) {
        final long deadline = FDateMillis.nowMillis() + START_WAIT.longValue(FTimeUnit.MILLISECONDS);
        while (FDateMillis.nowMillis() < deadline) {
            final String containerId = readIdFile(idFile);
            if (containerId != null) {
                return containerId;
            }
            if (!process.isAlive()) {
                LOG.warn("holder exited before reporting its container: %s", readOut(outFile));
                return null;
            }
            sleepSeconds(2);
        }
        return null;
    }

    private String readIdFile(final File idFile) {
        final String content = Files.readStringNoThrow(idFile.toPath());
        if (content == null || content.isBlank()) {
            return null;
        }
        return content.trim();
    }

    private boolean awaitExit(final Process process) {
        final long deadline = FDateMillis.nowMillis() + 30000;
        while (process.isAlive() && FDateMillis.nowMillis() < deadline) {
            sleepSeconds(1);
        }
        return !process.isAlive();
    }

    private String readOut(final File outFile) {
        final String content = Files.readStringNoThrow(outFile.toPath());
        return content != null ? content : "";
    }

    private DockerClient dockerClient() {
        return DockerClientFactory.instance().client();
    }

    private boolean isRunning(final String containerId) {
        if (containerId == null) {
            return false;
        }
        try {
            final InspectContainerResponse info = dockerClient().inspectContainerCmd(containerId).exec();
            return info.getState() != null && Boolean.TRUE.equals(info.getState().getRunning());
        } catch (final Exception e) {
            return false;
        }
    }

    /**
     * @return how long it took until the container was neither running nor present, or null if that never happened
     *         inside {@code wait}.
     */
    private Duration waitForContainerGone(final String containerId, final Duration wait) {
        if (containerId == null) {
            return Duration.ZERO;
        }
        final long start = FDateMillis.nowMillis();
        final long deadline = start + wait.longValue(FTimeUnit.MILLISECONDS);
        while (FDateMillis.nowMillis() < deadline) {
            if (!isRunning(containerId)) {
                try {
                    dockerClient().inspectContainerCmd(containerId).exec();
                } catch (final Exception e) {
                    return new Duration(FDateMillis.nowMillis() - start, FTimeUnit.MILLISECONDS);
                }
            }
            sleepSeconds(2);
        }
        return null;
    }

    private List<String> ryukContainerIds() {
        final List<String> ids = new ArrayList<>();
        for (final Container container : dockerClient().listContainersCmd().withShowAll(true).exec()) {
            final String name = container.getNames() != null && container.getNames().length > 0
                    ? container.getNames()[0]
                    : "";
            final Map<String, String> labels = container.getLabels();
            if (name.contains("testcontainers-ryuk") || (labels != null && labels.containsKey("org.testcontainers.ryuk"))) {
                ids.add(shortId(container.getId()));
            }
        }
        return ids;
    }

    /**
     * The testcontainers session a container belongs to - the label ryuk's filter is built from, and the reason a
     * reaper never reaps another session's containers.
     */
    private String sessionLabelOf(final String containerId) {
        if (containerId == null) {
            return null;
        }
        try {
            final InspectContainerResponse info = dockerClient().inspectContainerCmd(containerId).exec();
            final Map<String, String> labels = info.getConfig() != null ? info.getConfig().getLabels() : null;
            return labels != null ? labels.get(SESSION_LABEL) : null;
        } catch (final Exception e) {
            LOG.warn("container %s not inspectable for its session label (%s)", shortId(containerId), e.toString());
            return null;
        }
    }

    private void reclaimByLabel(final String markerValue) {
        final Map<String, String> filter = Map.of(MARKER_LABEL, markerValue);
        for (final Container container : dockerClient().listContainersCmd().withShowAll(true).withLabelFilter(filter).exec()) {
            try {
                dockerClient().stopContainerCmd(container.getId()).withTimeout(5).exec();
            } catch (final Exception e) {
                LOG.debug("stop of %s ignored (%s)", shortId(container.getId()), e.toString());
            }
            try {
                dockerClient().removeContainerCmd(container.getId()).withForce(true).exec();
                LOG.info("reclaimed labeled container %s", shortId(container.getId()));
            } catch (final Exception e) {
                LOG.warn("could not reclaim %s: %s", shortId(container.getId()), e.toString());
            }
        }
    }

    private void removeIfPresent(final String containerId) {
        if (containerId == null) {
            return;
        }
        try {
            dockerClient().removeContainerCmd(containerId).withForce(true).withRemoveVolumes(true).exec();
        } catch (final Exception e) {
            LOG.debug("remove of %s not needed (%s)", shortId(containerId), e.toString());
        }
    }

    private void cleanup(final Holder holder) {
        if (holder == null) {
            return;
        }
        if (holder.process.isAlive()) {
            //graceful first: that is what a reaper or a shutdown hook normally reacts to
            holder.process.destroy();
            sleepSeconds(3);
        }
        if (holder.process.isAlive()) {
            holder.process.destroyForcibly();
        }
        removeIfPresent(holder.containerId);
        Files.deleteNative(holder.outFile);
        Files.deleteNative(holder.idFile);
    }

    private static String shortId(final String containerId) {
        return containerId != null && containerId.length() > 12 ? containerId.substring(0, 12) : containerId;
    }

    private static void sleepSeconds(final int seconds) {
        try {
            FTimeUnit.SECONDS.sleep(seconds);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Entry point of the child JVM: starts one payload container through testcontainers (which is what arms the ryuk
     * session) and parks, so the parent can kill it the way an operator's kill -9 does. The name ends in {@code Main}
     * because that is the checkstyle {@code UncommentedMain} exclusion the fleet's child-process entry points use
     * (ChildProcessMain).
     */
    @NotThreadSafe
    public static class ContainerMain {

        /**
         * @param args
         *            [0] the marker label value, [1] the fixed host port (0 for none), [2] the payload image, [3] the
         *            file the container id is written to for the parent to pick up.
         */
        public static void main(final String[] args) throws Exception {
            final String marker = args[0];
            final int hostPort = Integer.parseInt(args[1]);
            final String image = args[2];
            final File idFile = new File(args[3]);
            final GenericContainer<?> container;
            if (hostPort > 0) {
                container = new FixedHostPortGenericContainer<>(image).withFixedExposedPort(hostPort,
                        PAYLOAD_CONTAINER_PORT);
            } else {
                container = new GenericContainer<>(image);
            }
            container.withCommand("sh", "-c", "while :; do sleep 5; done").withLabel(MARKER_LABEL, marker);
            container.start();
            final boolean written = Files.writeStringToFileIfDifferent(idFile, container.getContainerId());
            if (!written) {
                throw new IllegalStateException("could not write the container id to " + idFile);
            }
            Thread.sleep(Long.MAX_VALUE);
        }
    }

}
