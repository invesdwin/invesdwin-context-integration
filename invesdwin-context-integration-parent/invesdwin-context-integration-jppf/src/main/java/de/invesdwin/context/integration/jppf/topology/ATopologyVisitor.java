package de.invesdwin.context.integration.jppf.topology;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.monitoring.topology.AbstractTopologyComponent;
import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.client.monitoring.topology.TopologyPeer;

import de.invesdwin.util.error.UnknownArgumentException;

@Immutable
public abstract class ATopologyVisitor {

    public void process(final TopologyManager manager) {
        // iterate over the discovered drivers
        final Set<String> duplicateDriverUuidFilter = new HashSet<>();
        final Set<String> duplicateNodeUuidFilter = new HashSet<>();
        for (final TopologyDriver driver : manager.getDrivers()) {
            processComponents(manager, driver, duplicateDriverUuidFilter, duplicateNodeUuidFilter);
            //discover hidden nodes that are only accessible via node forwarding
            final List<TopologyNode> hiddenNodes = TopologyDrivers.discoverHiddenNodes(driver);
            for (final TopologyNode hiddenNode : hiddenNodes) {
                processComponents(manager, hiddenNode, duplicateDriverUuidFilter, duplicateNodeUuidFilter);
            }
        }
        /*
         * Sometimes nodes are not listed properly under drivers (maybe a race condition), but instead only appear in
         * the nodes list. So we iterate over that as well. Duplicate UUID filter saves us from visiting them more than
         * once.
         */
        for (final TopologyNode node : manager.getNodes()) {
            processComponents(manager, node, duplicateDriverUuidFilter, duplicateNodeUuidFilter);
        }
    }

    protected abstract void visitDriver(TopologyDriver driver);

    protected abstract void visitNode(TopologyNode node);

    private void processComponents(final TopologyManager manager, final AbstractTopologyComponent comp,
            final Set<String> duplicateDriverUuidFilter, final Set<String> duplicateNodeUuidFilter) {
        if (comp.isDriver()) {
            if (!duplicateDriverUuidFilter.add(comp.getUuid())) {
                return;
            }
            final TopologyDriver driver = (TopologyDriver) comp;
            visitDriver(driver);
        } else if (comp.isNode()) {
            final TopologyNode node = (TopologyNode) comp;
            if (!duplicateNodeUuidFilter.add(comp.getUuid())) {
                return;
            }
            visitNode(node);
        } else if (comp.isPeer()) {
            final TopologyPeer peer = (TopologyPeer) comp;
            // retrieve the actual driver the peer refers to
            final TopologyDriver actualDriver = manager.getDriver(peer.getUuid());
            if (!duplicateDriverUuidFilter.add(comp.getUuid())) {
                return;
            }
            visitDriver(actualDriver);
        } else {
            throw UnknownArgumentException.newInstance(AbstractTopologyComponent.class, comp);
        }
        for (final AbstractTopologyComponent child : comp.getChildren()) {
            processComponents(manager, child, duplicateDriverUuidFilter, duplicateNodeUuidFilter);
        }
    }

}
