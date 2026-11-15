package de.invesdwin.context.integration.grid.docker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.Immutable;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.math.Integers;

/**
 * Claims a single-owner container slot by removing whoever else holds it: stop and remove every pre-existing container
 * carrying the service's docker label, including the orphan a JVM that died ungracefully leaves behind, which otherwise
 * becomes a second container holding the slot.
 *
 * Only labeled containers are ever touched: the list call filters by the label and the selection re-checks it locally
 * (see {@link ContainerOwnership}), so a wrong filter can not delete a foreign container.
 *
 * The claim has to be race tolerant and it has to be the part that survives, because the testcontainers reaper is not:
 * a reaper only reaps containers of its own session (it filters on the session id and the reaper container itself is
 * named per session), so a labeled holder whose session died is only ever reaped by that old session's own reaper -
 * and never at all after a crash boot, a docker daemon restart or an operator-started container. Meanwhile the old
 * reaper can still be killing that orphan while this claim runs (measured 2026-11-05: the reaper killed a kill -9
 * orphan about a second after its client died, and our stop/remove landed three seconds later), so gone and
 * mid-removal answers from docker are normal here: they are re-checked by label instead of being reported as a failed
 * reclaim. And because testcontainers caches a failed reaper handshake in its client factory for the life of the JVM
 * (measured the same day: the first call burned the 30 s reaper timeout, the next failed 1 ms later, the third after
 * 0 ms), the claim takes the client through a supplier - a client that can not be built is a warning, never an
 * exception thrown at the caller.
 */
@Immutable
public final class ContainerReclaimer {

    private static final Log LOG = new Log(ContainerReclaimer.class);
    private static final int STOP_TIMEOUT_SECONDS = 5;
    /** first sweep plus re-checks, for a holder the docker daemon was still removing under us. */
    private static final int SWEEP_ROUNDS = 3;

    private ContainerReclaimer() {}

    /**
     * Stops and removes every pre-existing container owned by {@code ownership}, leaving the docker host at single owner
     * before a new container is started. A failure to reclaim one container is logged and does not abort the startup:
     * the following container start then fails visibly (port conflict) and goes through the caller's own retry path.
     *
     * @param reason only for the log (startup, start failure, cleanup)
     */
    public static void reclaimPreExistingContainers(final ContainerOwnership ownership, final DockerClient dockerClient,
            final String reason) {
        reclaimPreExistingContainers(ownership, dockerClient, reason, false);
    }

    /**
     * @param dockerClient supplier so that a docker client that can not be acquired (a cached reaper-handshake failure
     *            poisons every later docker call in this JVM) is reported here instead of being thrown at the caller.
     */
    public static void reclaimPreExistingContainers(final ContainerOwnership ownership,
            final Supplier<DockerClient> dockerClient, final String reason) {
        reclaimPreExistingContainers(ownership, dockerClient, reason, false);
    }

    /**
     * @param warnWhenFound escalate the naming log line from INFO to WARN: used after a failed container start, where a
     *            pre-existing labeled holder of the fixed ports is the expected culprit (port already allocated, or a
     *            port wait timeout against a container that is not ours).
     */
    public static void reclaimPreExistingContainers(final ContainerOwnership ownership, final DockerClient dockerClient,
            final String reason, final boolean warnWhenFound) {
        final Set<String> reclaimedIds = ILockCollectionFactory.getInstance(false).newSet();
        final List<Container> owned = listOwnedContainers(ownership, dockerClient, reason);
        if (owned == null) {
            //the lookup itself failed, already reported
            return;
        }
        int foundCount = 0;
        List<Container> current = owned;
        int round = 0;
        while (!current.isEmpty() && round < SWEEP_ROUNDS) {
            round++;
            foundCount = Integers.max(foundCount, current.size());
            boolean maybeLeft = false;
            for (final Container container : current) {
                maybeLeft |= reclaim(dockerClient, container, reclaimedIds);
            }
            if (!maybeLeft) {
                current = List.of();
                break;
            }
            //a holder that docker was still removing (concurrent reaper) may still own the fixed ports
            final List<Container> again = listOwnedContainers(ownership, dockerClient, reason + " recheck " + round);
            current = again != null ? again : List.of();
        }
        if (round == 0) {
            LOG.debug("No pre-existing container to reclaim for %s (%s)", ownership.getLabel(), reason);
            return;
        }
        if (!current.isEmpty()) {
            LOG.warn("%s pre-existing container(s) labelled %s still hold the fixed ports after %s reclaim sweep(s) "
                    + "(%s): %s", current.size(), ownership.getLabel(), round, reason, describe(current));
        }
        if (warnWhenFound) {
            LOG.warn("Reclaimed %s of %s pre-existing container(s) labelled %s (%s): %s", reclaimedIds.size(),
                    foundCount, ownership.getLabel(), reason, String.join(", ", reclaimedIds));
        } else {
            LOG.info("Reclaimed %s of %s pre-existing container(s) labelled %s (%s): %s", reclaimedIds.size(),
                    foundCount, ownership.getLabel(), reason, String.join(", ", reclaimedIds));
        }
    }

    /**
     * @param dockerClient supplier so that a docker client that can not be acquired is reported here, not thrown.
     * @see #reclaimPreExistingContainers(ContainerOwnership, DockerClient, String, boolean)
     */
    public static void reclaimPreExistingContainers(final ContainerOwnership ownership,
            final Supplier<DockerClient> dockerClient, final String reason, final boolean warnWhenFound) {
        final DockerClient client;
        try {
            client = dockerClient.get();
        } catch (final Exception e) {
            LOG.warn("Can not claim the container slot labelled %s (%s): the docker client is not usable (%s): %s",
                    ownership.getLabel(), reason, e.getClass().getSimpleName(), e);
            return;
        }
        reclaimPreExistingContainers(ownership, client, reason, warnWhenFound);
    }

    static boolean isRunning(final String state) {
        return "running".equals(state);
    }

    private static List<Container> listOwnedContainers(final ContainerOwnership ownership,
            final DockerClient dockerClient, final String reason) {
        final Map<String, String> labelFilter = ownership.newLabelFilter();
        try {
            final List<Container> listed = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withLabelFilter(labelFilter)
                    .exec();
            return ownership.selectOwned(listed);
        } catch (final Exception e) {
            LOG.warn("Can not look up pre-existing containers labelled %s (%s): %s: %s", ownership.getLabel(),
                    reason, e.getClass().getSimpleName(), e);
            return null;
        }
    }

    private static String describe(final List<Container> containers) {
        final List<String> ids = new ArrayList<>();
        for (final Container container : containers) {
            ids.add(ContainerOwnership.shortId(container.getId()));
        }
        return String.join(", ", ids);
    }

    /**
     * @return true if the container may still exist afterwards, which asks for a re-check of the label lookup
     */
    private static boolean reclaim(final DockerClient dockerClient, final Container container,
            final Set<String> reclaimedIds) {
        final String containerId = container.getId();
        if (isRunning(container.getState())) {
            try {
                dockerClient.stopContainerCmd(containerId).withTimeout(STOP_TIMEOUT_SECONDS).exec();
            } catch (final NotFoundException e) {
                //the old session's reaper or an operator removed it in between: nothing left to stop
                LOG.debug("Container %s was already gone before the stop (%s)",
                        ContainerOwnership.shortId(containerId), e);
            } catch (final Exception e) {
                //an already stopped or mid-removal container is still claimable by force below
                LOG.debug("Stop of container %s ignored (%s)", ContainerOwnership.shortId(containerId), e);
            }
        }
        try {
            //force covers the case where the stop timed out but the container still holds the fixed host ports
            dockerClient.removeContainerCmd(containerId).withForce(true).exec();
            reclaimedIds.add(ContainerOwnership.shortId(containerId));
            return false;
        } catch (final NotFoundException e) {
            //reaped under us: the slot is claimed either way
            reclaimedIds.add(ContainerOwnership.shortId(containerId));
            LOG.debug("Container %s was already gone before the remove (%s)",
                    ContainerOwnership.shortId(containerId), e);
            return false;
        } catch (final ConflictException e) {
            //docker is still removing it (the reaper won the race for the same container) - re-check the lookup
            LOG.debug("Container %s is being removed already, re-checking (%s)",
                    ContainerOwnership.shortId(containerId), e);
            return true;
        } catch (final Exception e) {
            LOG.warn("Could not reclaim container %s (%s): %s", ContainerOwnership.shortId(containerId),
                    e.getClass().getSimpleName(), e);
            return true;
        }
    }

}
