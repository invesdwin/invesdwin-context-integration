package de.invesdwin.context.integration.batch;

import java.util.Set;

/**
 * As a workaround for being able to disable auto loading of batch job xmls.
 * 
 * @author subes
 * 
 */
public interface IDisabledBatchContext {

    Set<String> getResourceNames();

}
