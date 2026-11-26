package de.invesdwin.context.integration.grid.docker;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.Immutable;

import com.github.dockerjava.api.DockerClient;

import de.invesdwin.context.integration.retry.ExponentialBackoff;
import de.invesdwin.context.integration.retry.fast.FastNonBlockingRetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.millis.FDateMillis;
import de.invesdwin.util.time.duration.Duration;

/**
 * The start half of a single-owner docker slot claim. The reclaim that runs before the first bind of the fixed host
 * ports (see {@link ContainerReclaimer}) does not cover a failed start: this JVM is then left choosing between storming
 * straight back into the same failure or giving up. Neither - every retry here first runs the labeled reclaim (stop and
 * remove, naming what it removed) so no labeled orphan, a crash-boot leftover or a container a previous attempt
 * half-left, holds the fixed ports when the next attempt binds; then it waits with the shared bounded
 * {@link ExponentialBackoff} ladder and gives up after the caller's attempt bound, so the caller's own retry ladder
 * stays the outer bound.
 *
 * A testcontainers ryuk reaper handshake failure ("Could not connect to Ryuk at host:port") is the one failure that
 * must not be retried in this JVM: testcontainers 2.0.3 stores the failed handshake in its client factory
 * (cachedClientFailure) and rethrows it from every later docker call, so a further start would fail instantly without
 * ever reaching docker again (measured 2026-11-05: the first call burned the 30 s reaper timeout, the next failed 1 ms
 * later, the third after 0 ms - a reconnect storm of thousands of 'Can not connect to Ryuk' lines over a service that
 * could never bind its ports is that loop seen from the outside). So the attempt ends here with a warning naming the
 * cause; ryuk is never disabled by us, the labeled reclaim stays the single-owner guard either way, and the caller's
 * next attempt is what gets a fresh handshake.
 */
@Immutable
public final class ContainerStarter {

    private static final Log LOG = new Log(ContainerStarter.class);
    //the markers of the ryuk reaper's IllegalStateException after its own connect timeout expired
    private static final String[] RYUK_HANDSHAKE_MARKERS = { "Could not connect to Ryuk", "Can not connect to Ryuk" };
    //container start retries wait shortly (this is inside one attempt), capped so the ladder stays bounded
    private static final Duration RETRY_MIN_BACKOFF = Duration.ONE_SECOND;
    private static final Duration RETRY_MAX_BACKOFF = new Duration(10, FTimeUnit.SECONDS);

    private ContainerStarter() {}

    /**
     * @param ownership the label of the slot whose orphan has to be gone before {@code startAction} re-binds its ports
     * @param startAction the container start (and whatever connects to it) to be retried under the claim
     * @param dockerClient supplier used for the reclaim between attempts, kept a supplier because a client that can not
     *            be built is not a reason to fail the start
     * @param maxAttempts bounded so the caller's own retry ladder stays the outer bound
     */
    public static void startWithSingleOwnerClaim(final ContainerOwnership ownership, final Runnable startAction,
            final Supplier<DockerClient> dockerClient, final int maxAttempts) {
        startWithSingleOwnerClaim(ownership, startAction, dockerClient, maxAttempts, millis -> {
            try {
                new Duration(millis, FTimeUnit.MILLISECONDS).sleep();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FastNonBlockingRetryLaterRuntimeException("container start retry interrupted", e);
            }
        });
    }

    /**
     * @param retrySleep the wait between attempts, a parameter so the offline test can assert the ladder without waiting
     */
    static void startWithSingleOwnerClaim(final ContainerOwnership ownership, final Runnable startAction,
            final Supplier<DockerClient> dockerClient, final int maxAttempts, final LongConsumer retrySleep) {
        Assertions.checkArgument(maxAttempts >= 1);
        final ExponentialBackoff retryBackoff = new ExponentialBackoff(RETRY_MIN_BACKOFF, RETRY_MAX_BACKOFF);
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                startAction.run();
                return;
            } catch (final RuntimeException e) {
                lastFailure = e;
                if (isRyukHandshakeFailure(e)) {
                    LOG.warn("The testcontainers ryuk reaper handshake failed on this docker host (%s), attempt %s of "
                            + "%s: testcontainers keeps that failure for the life of this JVM, so no further start here "
                            + "can reach docker again - ending this attempt instead of retrying it; ryuk stays enabled, "
                            + "the labeled reclaim of the next attempt is the single-owner guard", e.getMessage(),
                            attempt, maxAttempts);
                    break;
                }
                LOG.warn("Container start for slot %s failed with %s: %s (attempt %s of %s)", ownership.getLabel(),
                        e.getClass().getSimpleName(), e.getMessage(), attempt, maxAttempts);
                if (attempt < maxAttempts) {
                    //whatever the failure: claim the slot by label before the next attempt re-binds the fixed ports
                    ContainerReclaimer.reclaimPreExistingContainers(ownership, dockerClient,
                            "start failure " + attempt, true);
                    final Duration gap = retryBackoff.recordRestart(FDateMillis.nowMillis());
                    LOG.warn("Retrying container start for slot %s after %s", ownership.getLabel(), gap);
                    retrySleep.accept(gap.longValue(FTimeUnit.MILLISECONDS));
                }
            }
        }
        //maxAttempts >= 1, so a failure was recorded
        throw lastFailure;
    }

    /**
     * The reaper reports its failed handshake as an {@link IllegalStateException} naming the marker; the cause chain is
     * searched because the start wraps it (testcontainers puts it under a {@code ContainerLaunchException}).
     */
    static boolean isRyukHandshakeFailure(final Throwable t) {
        for (Throwable cause = t; cause != null && cause != cause.getCause(); cause = cause.getCause()) {
            if (!(cause instanceof IllegalStateException) || cause.getMessage() == null) {
                continue;
            }
            for (final String marker : RYUK_HANDSHAKE_MARKERS) {
                if (cause.getMessage().contains(marker)) {
                    return true;
                }
            }
        }
        return false;
    }

}
