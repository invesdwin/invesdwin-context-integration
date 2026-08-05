package de.invesdwin.context.integration.grid.ignite2.server;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.context.beans.init.locations.AConditionalContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.position.ResourcePosition;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import jakarta.inject.Named;

/**
 * Webserver should only be started explicitly.
 * 
 */
@ThreadSafe
@Named
public class Ignite2ServerContextLocation extends AConditionalContextLocation {

    public static final PositionedResource SERVER_CONTEXT_LOCATION;

    private static volatile boolean activated = Ignite2ServerProperties.STARTUP_ENABLED;

    static {
        Assertions.checkNotNull(PreMergedContext.getInstance());
        SERVER_CONTEXT_LOCATION = PositionedResource.of(new ClassPathResource("/META-INF/ctx.ignite2.server.xml"),
                ResourcePosition.START);
    }

    @Override
    protected List<PositionedResource> getContextResourcesIfConditionSatisfied() {
        return Arrays.asList(SERVER_CONTEXT_LOCATION);
    }

    @Override
    protected boolean isConditionSatisfied() {
        return activated;
    }

    public static void activate() {
        activated = true;
    }

    public static void deactivate() {
        activated = false;
    }

    public static boolean isActivated() {
        return activated;
    }

}
