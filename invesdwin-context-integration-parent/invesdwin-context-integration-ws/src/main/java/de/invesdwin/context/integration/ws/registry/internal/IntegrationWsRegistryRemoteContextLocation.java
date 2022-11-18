package de.invesdwin.context.integration.ws.registry.internal;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.ABeanDependantContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.util.collections.Arrays;
import jakarta.inject.Named;

@Named
@Immutable
public class IntegrationWsRegistryRemoteContextLocation extends ABeanDependantContextLocation {

    @Override
    protected List<PositionedResource> getContextResourcesIfBeanExists() {
        return null;
    }

    @Override
    protected List<PositionedResource> getContextResourcesIfBeanNotExists() {
        return Arrays.asList(
                PositionedResource.of(new ClassPathResource("/META-INF/ctx.integration.ws.registry.remote.xml")));
    }

    @Override
    protected Class<?> getDependantBeanType() {
        return IRegistryService.class;
    }

}
