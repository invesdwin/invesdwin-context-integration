package de.invesdwin.context.integration.ws.registry.internal;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.ABeanDependantContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.ws.registry.IRegistryService;

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
