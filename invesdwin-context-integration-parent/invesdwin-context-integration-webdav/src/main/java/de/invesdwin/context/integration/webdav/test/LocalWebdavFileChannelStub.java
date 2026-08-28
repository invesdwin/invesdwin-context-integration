package de.invesdwin.context.integration.webdav.test;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.beans.hook.ReinitializationHookManager;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.ws.registry.RegistryServiceStub;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@NotThreadSafe
public class LocalWebdavFileChannelStub extends StubSupport {

    private static final Log LOG = new Log(LocalWebdavFileChannelStub.class);

    private static boolean reinitizationHookRegistered = false;

    @Override
    public void setUpContextBeforeLoading(final ATest test) throws Exception {
        if (!RegistryServiceStub.isEnabled()) {
            return;
        }
        LOG.warn("Registering %s to disable webdav communication for local testing.",
                LocalWebdavFileChannel.class.getSimpleName());
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
