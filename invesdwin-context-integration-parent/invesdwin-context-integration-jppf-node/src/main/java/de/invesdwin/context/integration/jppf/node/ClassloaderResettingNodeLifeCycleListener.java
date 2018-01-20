package de.invesdwin.context.integration.jppf.node;

import javax.annotation.concurrent.Immutable;

import org.jppf.node.event.NodeLifeCycleEvent;
import org.jppf.node.event.NodeLifeCycleListenerAdapter;

@Immutable
public class ClassloaderResettingNodeLifeCycleListener extends NodeLifeCycleListenerAdapter {

    /**
     * Use a fresh classloader for each job to prevent classcastexceptions from occuring
     */
    @Override
    public void jobHeaderLoaded(final NodeLifeCycleEvent event) {
        event.getNode().resetTaskClassLoader();
    }

    @Override
    public void beforeNextJob(final NodeLifeCycleEvent event) {
        event.getNode().resetTaskClassLoader();
    }

}
