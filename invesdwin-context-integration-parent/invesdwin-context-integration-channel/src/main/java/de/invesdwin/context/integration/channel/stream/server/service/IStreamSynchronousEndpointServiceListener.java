package de.invesdwin.context.integration.channel.stream.server.service;

import de.invesdwin.context.system.properties.IProperties;

public interface IStreamSynchronousEndpointServiceListener {

    IStreamSynchronousEndpointServiceListener[] EMPTY_ARRAY = new IStreamSynchronousEndpointServiceListener[0];

    void onPut() throws Exception;

    void onDelete(IProperties parameters) throws Exception;

}
