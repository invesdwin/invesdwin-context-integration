package de.invesdwin.context.integration.jppf.node;

import javax.annotation.concurrent.Immutable;

import org.jppf.node.event.NodeLifeCycleEvent;
import org.jppf.node.event.NodeLifeCycleListenerAdapter;

import de.invesdwin.context.integration.jppf.RemoteFastJPPFSerialization;
import de.invesdwin.context.log.Log;

@Immutable
public class ClassLoaderResettingNodeLifeCycleListener extends NodeLifeCycleListenerAdapter {

    private final Log log = new Log(this);

    /**
     * Use a fresh classloader for each job to prevent classcastexceptions from occuring
     */
    @Override
    public void jobHeaderLoaded(final NodeLifeCycleEvent event) {
        log.info("Preparing for next job");
        if (JPPFNodeProperties.RESET_TASK_CLASS_LOADER) {
            event.getNode().resetTaskClassLoader();
        }
        RemoteFastJPPFSerialization.refresh();
    }

    @Override
    public void beforeNextJob(final NodeLifeCycleEvent event) {
        log.info("Finished with job");
    }

}
