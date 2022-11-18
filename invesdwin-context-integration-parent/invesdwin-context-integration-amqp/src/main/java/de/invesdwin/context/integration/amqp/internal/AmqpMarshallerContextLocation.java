package de.invesdwin.context.integration.amqp.internal;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.ABeanDependantContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.marshaller.IMergedJaxbContextPath;
import de.invesdwin.util.collections.Arrays;
import jakarta.inject.Named;

@Named
@Immutable
public class AmqpMarshallerContextLocation extends ABeanDependantContextLocation {

    @Override
    protected List<PositionedResource> getContextResourcesIfBeanExists() {
        return Arrays
                .asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.integration.amqp.marshaller.xml")));
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
