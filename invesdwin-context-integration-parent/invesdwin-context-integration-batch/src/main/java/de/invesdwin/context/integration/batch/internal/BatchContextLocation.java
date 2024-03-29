package de.invesdwin.context.integration.batch.internal;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import jakarta.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.util.collections.Arrays;

@Named
@NotThreadSafe
public class BatchContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.integration.batch.xml")));
    }

}
