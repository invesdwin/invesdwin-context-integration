package de.invesdwin.context.integration.jms.internal;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.ABeanDependantContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.marshaller.IMergedJaxbContextPath;

@Named
@Immutable
public class JmsMarshallerContextLocation extends ABeanDependantContextLocation {

    @Override
    protected List<PositionedResource> getContextResourcesIfBeanExists() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.integration.jms.marshaller.xml")));
    }

    @Override
    protected List<PositionedResource> getContextResourcesIfBeanNotExists() {
        return null;
    }

    @Override
    protected Class<?> getDependantBeanType() {
        return IMergedJaxbContextPath.class;
    }

}
