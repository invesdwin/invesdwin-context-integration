package de.invesdwin.context.integration.amqp;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.util.collections.Arrays;

@ThreadSafe
public class AmqpIntegrationTestContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.integration.amqp.test.xml")));
    }

}
