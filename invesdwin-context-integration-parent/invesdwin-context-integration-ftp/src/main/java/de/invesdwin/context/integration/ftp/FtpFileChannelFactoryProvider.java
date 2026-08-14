package de.invesdwin.context.integration.ftp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactory;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactoryProvider;
import de.invesdwin.util.collections.Collections;

@Immutable
public class FtpFileChannelFactoryProvider implements IFileChannelFactoryProvider {

    public static final String[] SCHEMES = new String[] { "ftp", "ftps", "ftpes" };

    @Override
    public Collection<IFileChannelFactory> newFactories() {
        final List<IFileChannelFactory> factories = new ArrayList<>(SCHEMES.length);
        for (int i = 0; i < SCHEMES.length; i++) {
            final String scheme = SCHEMES[i];
            factories.add(new FtpFileChannelFactory(scheme));
        }
        return Collections.unmodifiableCollection(factories);
    }

}