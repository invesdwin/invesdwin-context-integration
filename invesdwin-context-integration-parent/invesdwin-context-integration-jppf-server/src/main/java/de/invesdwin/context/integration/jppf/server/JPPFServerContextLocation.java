package de.invesdwin.context.integration.jppf.server;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.AConditionalContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.position.ResourcePosition;

/**
 * Webserver should only be started explicitly.
 * 
 */
@ThreadSafe
@Named
public class JPPFServerContextLocation extends AConditionalContextLocation {

    public static final PositionedResource CONTEXT_LOCATION = PositionedResource
            .of(new ClassPathResource("/META-INF/ctx.jppf.server.xml"), ResourcePosition.START);

    private static volatile boolean activated = JPPFServerProperties.STARTUP_ENABLED;

    @Override
    protected List<PositionedResource> getContextResourcesIfConditionSatisfied() {
        return Arrays.asList(CONTEXT_LOCATION);
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
