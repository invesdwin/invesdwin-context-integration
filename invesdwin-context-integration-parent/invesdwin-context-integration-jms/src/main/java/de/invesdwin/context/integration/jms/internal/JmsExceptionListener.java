package de.invesdwin.context.integration.jms.internal;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import de.invesdwin.context.log.error.Err;

@Named
@Immutable
public class JmsExceptionListener implements ExceptionListener {

    @Override
    public void onException(final JMSException exception) {
        Err.process(exception);
    }

}
