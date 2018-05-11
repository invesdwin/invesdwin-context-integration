package de.invesdwin.context.integration.quasar.internal;

import javax.annotation.concurrent.Immutable;

import co.paralleluniverse.fibers.instrument.LogLevel;
import de.invesdwin.context.log.Log;

@Immutable
public class QuasarLog implements co.paralleluniverse.fibers.instrument.Log {

    private final Log log = new Log(this);

    @Override
    public void log(final LogLevel level, final String msg, final Object... args) {
        switch (level) {
        case DEBUG:
            log.debug(msg, args);
        case INFO:
            log.info(msg, args);
        case WARNING:
            log.warn(msg, args);
            break;
        default:
            log.error(msg, args);
            break;
        }
    }

    @Override
    public void error(final String msg, final Throwable ex) {
        log.error(msg, ex);
    }
}