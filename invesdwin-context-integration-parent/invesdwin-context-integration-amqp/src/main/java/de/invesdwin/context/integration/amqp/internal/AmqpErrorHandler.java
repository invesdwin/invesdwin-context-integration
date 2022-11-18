package de.invesdwin.context.integration.amqp.internal;

import javax.annotation.concurrent.Immutable;

import org.springframework.util.ErrorHandler;

import de.invesdwin.context.log.error.Err;
import jakarta.inject.Named;

@Immutable
@Named
public class AmqpErrorHandler implements ErrorHandler {

    @Override
    public void handleError(final Throwable t) {
        Err.process(t);
    }

}
