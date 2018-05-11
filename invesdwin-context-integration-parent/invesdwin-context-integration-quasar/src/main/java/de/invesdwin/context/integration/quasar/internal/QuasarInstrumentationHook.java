package de.invesdwin.context.integration.quasar.internal;

import java.lang.instrument.Instrumentation;

import javax.annotation.concurrent.NotThreadSafe;

import co.paralleluniverse.fibers.instrument.JavaAgent;
import co.paralleluniverse.fibers.instrument.LogLevel;
import co.paralleluniverse.fibers.instrument.Retransform;
import de.invesdwin.context.beans.hook.IInstrumentationHook;
import de.invesdwin.context.log.Log;

@NotThreadSafe
public class QuasarInstrumentationHook implements IInstrumentationHook {

    private final Log log = new Log(this);

    @Override
    public void instrument(final Instrumentation instrumentation) {
        JavaAgent.agentmain("mb", instrumentation);
        Retransform.getInstrumentor().setLog(new co.paralleluniverse.fibers.instrument.Log() {
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
        });
    }

}
