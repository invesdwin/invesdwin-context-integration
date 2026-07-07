package net.sf.webdav.impl;

import javax.annotation.concurrent.Immutable;

import jakarta.activation.MimetypesFileTypeMap;
import net.sf.webdav.spi.IMimeTyper;

@Immutable
public class ActivationMimeTyper implements IMimeTyper {

    @Override
    public String getMimeType(final String path) {
        return MimetypesFileTypeMap.getDefaultFileTypeMap().getContentType(path);
    }

}
