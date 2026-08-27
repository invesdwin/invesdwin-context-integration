package de.invesdwin.context.integration.webdav.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.beans.hook.ReinitializationHookSupport;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.webdav.WebdavFileChannelFactoryProvider;
import jakarta.inject.Named;

@Immutable
@Named
public class LocalWebdavFileChannelReinitializationHook extends ReinitializationHookSupport {

    @Override
    public void reinitializationStarted() {
        FileChannelRegistry.register(new WebdavFileChannelFactoryProvider());
    }

}
