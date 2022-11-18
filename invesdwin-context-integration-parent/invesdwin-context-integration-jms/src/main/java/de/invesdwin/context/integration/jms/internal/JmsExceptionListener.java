package de.invesdwin.context.integration.jms.internal;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.log.error.Err;
import jakarta.inject.Named;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;

@Named
@Immutable
public class JmsExceptionListener implements ExceptionListener {

    @Override
    public void onException(final JMSException exception) {
        Err.process(exception);
    }

}
