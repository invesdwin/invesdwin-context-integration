package de.invesdwin.context.integration.ftp;

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
public class FtpFileChannelFactoryProvider implements IFileChannelFactoryProvider {

    @Override
    public Collection<IFileChannelFactory> newFactories() {
        final List<IFileChannelFactory> factories = Arrays.asList(createFactory("ftp"), createFactory("ftps"),
                createFactory("ftpes"));
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
                return new FtpFileChannel(serverUri);
            }
        };
    }
}