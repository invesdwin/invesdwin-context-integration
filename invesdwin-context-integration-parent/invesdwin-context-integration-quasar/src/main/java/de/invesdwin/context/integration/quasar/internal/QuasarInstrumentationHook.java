package de.invesdwin.context.integration.quasar.internal;

import java.lang.instrument.Instrumentation;

import javax.annotation.concurrent.NotThreadSafe;

import co.paralleluniverse.fibers.instrument.JavaAgent;
import co.paralleluniverse.fibers.instrument.Retransform;
import de.invesdwin.context.beans.hook.IInstrumentationHook;

@NotThreadSafe
public class QuasarInstrumentationHook implements IInstrumentationHook {

    @Override
    public void instrument(final Instrumentation instrumentation) {
        JavaAgent.agentmain("mb", instrumentation);
        Retransform.getInstrumentor().setLog(new QuasarLog());
    }

}
