package de.invesdwin.context.integration.channel.rpc.service.command;

import java.io.Closeable;

public interface IServiceSynchronousCommand<M> extends Closeable {

    /**
     * "HEARTBEAT" in numbers is "834578347"
     */
    int HEARTBEAT_SERVICE_ID = -834578347;

    /**
     * "ERROR" in numbers is "35505"
     */
    int ERROR_METHOD_ID = -35505;

    /**
     * "RETRERROR" in numbers is "537535505"
     */
    int RETRY_ERROR_METHOD_ID = -537535505;

    int getService();

    int getMethod();

    int getSequence();

    M getMessage();

}
