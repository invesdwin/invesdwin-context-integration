package de.invesdwin.context.integration.grid.docker;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;

/**
 * The ownership marker is the lock on a single-owner docker slot, so its convention has to be pinned where it is
 * defined: key and value both the service name, stamped at container creation (before any start, which is what lets a
 * later reclaim recognise the container as ours), and recognised from the label map rather than from the lookup filter.
 * A filter that asks for something the guard rejects reclaims nothing in silence; a guard that accepts what the filter
 * does not ask for is dead code that reads like a safety net.
 */
@NotThreadSafe
public class ContainerOwnershipTest extends ATest {

    private static final String LABEL = "de.invesdwin.context.integration.grid.docker.testOwner";

    @Test
    public void stampedContainerIsOwnedBeforeItIsStarted() {
        //construction only: no docker daemon is contacted until start()
        final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("alpine:3.20"));
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        ownership.applyTo(container);
        //key and value are both the service name - the lookup filter and the local guard depend on that agreement
        Assertions.assertThat(container.getLabels()).containsEntry(LABEL, LABEL);
        Assertions.assertThat(ownership.owns(container.getLabels())).isTrue();
    }

    @Test
    public void lookupFilterAndGuardAskForTheSameThing() {
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        final Map<String, String> filter = ownership.newLabelFilter();
        Assertions.assertThat(filter).hasSize(1).containsEntry(LABEL, LABEL);
        for (final Map.Entry<String, String> entry : filter.entrySet()) {
            Assertions.assertThat(ownership.owns(labeled(entry.getKey(), entry.getValue()))).isTrue();
        }
    }

    @Test
    public void onlyOurKeyWithOurValueIsOwned() {
        final ContainerOwnership ownership = ContainerOwnership.of(LABEL);
        Assertions.assertThat(ownership.owns(labeled(LABEL, LABEL))).isTrue();
        //our key under a foreign value is someone else's container
        Assertions.assertThat(ownership.owns(labeled(LABEL, "some.other.owner"))).isFalse();
        //a foreign key carrying our name is not ours either
        Assertions.assertThat(ownership.owns(labeled("some.other.owner", LABEL))).isFalse();
        Assertions.assertThat(ownership.owns(ILockCollectionFactory.getInstance(false).newMap())).isFalse();
        Assertions.assertThat(ownership.owns(null)).isFalse();
    }

    @Test
    public void containerIsNamedByDockersShortId() {
        Assertions.assertThat(ContainerOwnership.shortId("deadbeefcafe1234")).isEqualTo("deadbeefcafe");
        //an id shorter than the convention is not padded
        Assertions.assertThat(ContainerOwnership.shortId("abc123")).isEqualTo("abc123");
        Assertions.assertThat(ContainerOwnership.shortId(null)).isEqualTo("?");
    }

    private static Map<String, String> labeled(final String key, final String value) {
        final Map<String, String> labels = ILockCollectionFactory.getInstance(false).newMap();
        labels.put(key, value);
        return labels;
    }

}
