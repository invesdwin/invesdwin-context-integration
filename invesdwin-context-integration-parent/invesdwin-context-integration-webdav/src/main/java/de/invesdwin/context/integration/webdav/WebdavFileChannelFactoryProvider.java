package de.invesdwin.context.integration.webdav;

import java.net.URI;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactory;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactoryProvider;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;

@Immutable
public class WebdavFileChannelFactoryProvider implements IFileChannelFactoryProvider {

    @Override
    public int getPriority() {
        return 10_000;
    }

    @Override
    public Collection<IFileChannelFactory> newFactories() {
        final List<IFileChannelFactory> factories = Arrays.asList(createFactory("webdav"), createFactory("dav"),
                createFactory("http"), createFactory("https"));
        return Collections.unmodifiableCollection(factories);
    }

    private IFileChannelFactory createFactory(final String scheme) {
        return new IFileChannelFactory() {
            @Override
            public String getScheme() {
                return scheme;
            }

            @Override
            public IFileChannel newInstance(final URI serverUri) {
                return new WebdavFileChannel(serverUri);
            }
        };
    }
}