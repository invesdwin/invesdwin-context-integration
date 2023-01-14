package de.invesdwin.context.integration.channel.sync.mina.type;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;

public interface IMinaSocketType {

    IoAcceptor newAcceptor();

    IoConnector newConnector();

    boolean isUnbindAcceptor();

    boolean isValidateConnect();

    boolean isNative();

}
