package de.invesdwin.context.integration.channel.sync.mina.type;

import java.util.concurrent.Executor;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;

public interface IMinaSocketType {

    IoAcceptor newAcceptor(Executor executor, int processorCount);

    IoConnector newConnector(Executor executor, int processorCount);

    boolean isUnbindAcceptor();

    boolean isValidateConnect();

    boolean isNative();

}
