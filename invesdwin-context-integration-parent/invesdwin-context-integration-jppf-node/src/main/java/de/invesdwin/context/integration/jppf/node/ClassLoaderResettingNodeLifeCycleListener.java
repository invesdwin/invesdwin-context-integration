package de.invesdwin.context.integration.jppf.node;

import javax.annotation.concurrent.Immutable;

import org.jppf.node.event.NodeLifeCycleEvent;
import org.jppf.node.event.NodeLifeCycleListenerAdapter;

import de.invesdwin.context.log.Log;

@Immutable
public class ClassLoaderResettingNodeLifeCycleListener extends NodeLifeCycleListenerAdapter {

    private final Log log = new Log(this);

    /**
     * Use a fresh classloader for each job to prevent classcastexceptions from occuring
     */
    @Override
    public void jobHeaderLoaded(final NodeLifeCycleEvent event) {
        log.info("Resetting task class loader");
        event.getNode().resetTaskClassLoader();
    }

}
