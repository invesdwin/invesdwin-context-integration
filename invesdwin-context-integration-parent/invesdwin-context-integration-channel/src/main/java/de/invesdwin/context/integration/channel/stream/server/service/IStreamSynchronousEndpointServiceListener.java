package de.invesdwin.context.integration.channel.stream.server.service;

import java.util.Map;

public interface IStreamSynchronousEndpointServiceListener {

    void onPut() throws Exception;

    void onDelete(Map<String, String> parameters) throws Exception;

}
