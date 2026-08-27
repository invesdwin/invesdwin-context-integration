package de.invesdwin.context.integration.webdav.test;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@NotThreadSafe
public class LocalWebdavFileChannelStub extends StubSupport {

    private static boolean reinitizationHookRegistered = false;

    @Override
    public void setUpContextBeforeLoading(final ATest test) throws Exception {
        maybeRegisterReinitializationHook();
        FileChannelRegistry.register(new LocalWebdavFileChannelFactoryProvider());
    }

    private static synchronized void maybeRegisterReinitializationHook() {
        if (!reinitizationHookRegistered) {
            ReinitializationHookManager.register(new LocalWebdavFileChannelReinitializationHook());
            reinitizationHookRegistered = true;
        }
    }
}
