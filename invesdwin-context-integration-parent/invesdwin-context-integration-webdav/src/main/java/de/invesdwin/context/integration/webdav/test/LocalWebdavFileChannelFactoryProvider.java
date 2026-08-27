package de.invesdwin.context.integration.webdav.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactory;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactoryProvider;
import de.invesdwin.context.integration.webdav.WebdavFileChannelFactoryProvider;
import de.invesdwin.util.collections.Collections;

@Immutable
public class LocalWebdavFileChannelFactoryProvider implements IFileChannelFactoryProvider {
    public static final String[] SCHEMES = WebdavFileChannelFactoryProvider.SCHEMES;

    @Override
    public Collection<IFileChannelFactory> newFactories() {
        final List<IFileChannelFactory> factories = new ArrayList<>(SCHEMES.length);
        for (int i = 0; i < SCHEMES.length; i++) {
            final String scheme = SCHEMES[i];
            factories.add(new LocalWebdavFileChannelFactory(scheme));
        }
        return Collections.unmodifiableCollection(factories);
    }
}