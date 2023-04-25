package de.invesdwin.context.integration.channel.sync.service.command;

import java.io.Closeable;

public interface IServiceSynchronousCommand<M> extends Closeable {

    /**
     * "HEARTBEAT" in numbers is "834578347"
     */
    int HEARTBEAT_SERVICE_ID = -834578347;

    int getService();

    int getMethod();

    int getSequence();

    M getMessage();

}
