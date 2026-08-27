package de.invesdwin.context.integration.ftp;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactory;

@Immutable
public class FtpFileChannelFactory implements IFileChannelFactory {
    private final String scheme;

    public FtpFileChannelFactory(final String scheme) {
        this.scheme = scheme;
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    @Override
    public IFileChannel newInstance(final URI serverUri) {
        //CHECKSTYLE:OFF
        return new FtpFileChannel(serverUri);
        //CHECKSTYLE:ON
    }
}