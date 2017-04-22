package de.invesdwin.context.integration.amqp.internal;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.springframework.util.ErrorHandler;

import de.invesdwin.context.log.error.Err;

@Immutable
@Named
public class AmqpErrorHandler implements ErrorHandler {

    @Override
    public void handleError(final Throwable t) {
        Err.process(t);
    }

}
