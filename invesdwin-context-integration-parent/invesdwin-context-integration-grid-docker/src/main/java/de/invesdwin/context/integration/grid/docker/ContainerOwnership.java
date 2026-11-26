package de.invesdwin.context.integration.grid.docker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.model.Container;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.math.Integers;

/**
 * The ownership marker for a docker slot that only one service instance may hold. The label is the lock: whoever
 * starts such a container stamps it, and whoever reclaims before binding filters by it, so the convention has to live
 * in exactly one place - key and value both the service name. A lookup filter is only a hint, so the selection
 * re-checks the label map of every listed container locally; that is what makes a wrong filter unable to touch a
 * foreign container.
 */
@Immutable
public final class ContainerOwnership {

    /** docker's own short-id length, used wherever a container is named in a log line. */
    private static final int SHORT_ID_LENGTH = 12;

    private final String label;

    private ContainerOwnership(final String label) {
        this.label = label;
    }

    public static ContainerOwnership of(final String label) {
        Assertions.checkNotNull(label);
        return new ContainerOwnership(label);
    }

    public String getLabel() {
        return label;
    }

    /**
     * Stamps the container at creation, before it is ever started, so that a later reclaim can recognise it as ours.
     */
    public void applyTo(final GenericContainer<?> container) {
        container.withLabel(label, label);
    }

    /**
     * The guard: a container is ours only when its labels carry this name as key AND as value. A foreign value under
     * our key is someone else's container, and a null map is an unlabelled one.
     */
    public boolean owns(final Map<String, String> labels) {
        return labels != null && label.equals(labels.get(label));
    }

    public List<Container> selectOwned(final List<Container> listed) {
        final List<Container> owned = new ArrayList<>();
        for (final Container container : listed) {
            if (owns(container.getLabels())) {
                owned.add(container);
            }
        }
        return owned;
    }

    /**
     * The single-entry filter for the docker list call.
     */
    public Map<String, String> newLabelFilter() {
        final Map<String, String> labelFilter = ILockCollectionFactory.getInstance(false).newMap();
        labelFilter.put(label, label);
        return labelFilter;
    }

    public static String shortId(final String containerId) {
        if (containerId == null) {
            return "?";
        }
        return containerId.substring(0, Integers.min(SHORT_ID_LENGTH, containerId.length()));
    }

    @Override
    public String toString() {
        return "ContainerOwnership [label=" + label + "]";
    }

}
