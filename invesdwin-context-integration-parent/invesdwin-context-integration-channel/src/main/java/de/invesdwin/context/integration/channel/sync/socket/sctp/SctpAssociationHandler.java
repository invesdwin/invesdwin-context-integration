package de.invesdwin.context.integration.channel.sync.socket.sctp;

import javax.annotation.concurrent.Immutable;

import com.sun.nio.sctp.AbstractNotificationHandler;
import com.sun.nio.sctp.AssociationChangeNotification;
import com.sun.nio.sctp.HandlerResult;
import com.sun.nio.sctp.ShutdownNotification;

@Immutable
public final class SctpAssociationHandler extends AbstractNotificationHandler<Void> {

    public static final SctpAssociationHandler INSTANCE = new SctpAssociationHandler();

    private SctpAssociationHandler() {}

    @Override
    public HandlerResult handleNotification(final AssociationChangeNotification not, final Void stream) {
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(final ShutdownNotification not, final Void stream) {
        return HandlerResult.RETURN;
    }
}