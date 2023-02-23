package de.invesdwin.context.integration.channel.sync.ucx.jucx;

import org.openucx.jucx.ucp.UcpWorker;

public interface IJucxSynchronousChannel {

    UcpWorker getUcpWorker();

    ErrorUcxCallback getErrorUcxCallback();

}
