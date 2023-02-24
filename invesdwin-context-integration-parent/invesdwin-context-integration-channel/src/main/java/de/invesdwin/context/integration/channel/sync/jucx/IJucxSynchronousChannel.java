package de.invesdwin.context.integration.channel.sync.jucx;

import org.openucx.jucx.ucp.UcpWorker;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

public interface IJucxSynchronousChannel extends ISynchronousChannel {

    UcpWorker getUcpWorker();

    ErrorUcxCallback getErrorUcxCallback();

}
