package de.invesdwin.common.integration.hadoop;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;

@Immutable
public class HadoopJobTestContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.hadoop.test.job.xml")));
    }

}
