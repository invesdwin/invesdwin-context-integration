package de.invesdwin.context.integration.jms.internal;

import javax.annotation.concurrent.Immutable;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import de.invesdwin.context.log.error.Err;
import jakarta.inject.Named;

@Named
@Immutable
public class JmsExceptionListener implements ExceptionListener {

    @Override
    public void onException(final JMSException exception) {
        Err.process(exception);
    }

}
