package de.invesdwin.context.integration.webdav.server.internal;

import java.util.List;

import javax.annotation.concurrent.Immutable;
import jakarta.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.util.collections.Arrays;

@Named
@Immutable
public class WebdavServerContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.webdav.server.xml")));
    }

}
