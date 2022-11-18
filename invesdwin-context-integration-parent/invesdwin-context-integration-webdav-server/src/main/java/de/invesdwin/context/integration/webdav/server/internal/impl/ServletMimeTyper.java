package de.invesdwin.context.integration.webdav.server.internal.impl;

import javax.annotation.concurrent.Immutable;

import jakarta.servlet.ServletContext;
import net.sf.webdav.spi.IMimeTyper;

@Immutable
public class ServletMimeTyper implements IMimeTyper {

    private final ServletContext servletContext;

    public ServletMimeTyper(final ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    @Override
    public String getMimeType(final String path) {
        return servletContext.getMimeType(path);
    }

}
