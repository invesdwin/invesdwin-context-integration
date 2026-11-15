package de.invesdwin.context.integration.channel.jgroups;

import javax.annotation.concurrent.NotThreadSafe;

import org.jgroups.Version;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * The suite needs a member a build can always discover, and this is it — the idiom
 * {@code de.invesdwin.context.integration.channel.darpc.DarpcChannelAvailabilityTest} established on 2026-11-15 for a
 * module whose real harnesses cannot run yet. Unlike darpc the missing precondition here is not a native library: the
 * JGroups channel adapter has not been written at all (issue #26), so this module has no main sources and its tests
 * would be assertions about nothing.
 * <p>
 * What is checked is therefore only what is genuinely true and would silently rot: that the pinned library resolves,
 * and that the channel test harness this module declares at test scope is actually on the test classpath. The third
 * cell is a tripwire, not a pass — it SKIPS while the adapter does not exist, so the day someone lands it the cell
 * starts checking that the class also carries the house naming and implements {@link ISynchronousChannel}.
 */
@NotThreadSafe
public class JgroupsChannelAvailabilityTest extends ATest {

    /**
     * The name the sibling modules' convention gives the first adapter class
     * (channel-jnetrobust ships {@code JnetrobustSynchronousChannel}); looked up reflectively because the class does not
     * exist yet, which is also what pins the naming decision in a build.
     */
    private static final String ADAPTER_CLASS_NAME =
            "de.invesdwin.context.integration.channel.jgroups.JgroupsSynchronousChannel";

    @Test
    public void testJgroupsLibraryIsLoadable() throws ClassNotFoundException {
        //the module depends on org.jgroups:jgroups at compile scope, so a build that resolves it but cannot load it is a
        //dependency problem, and it is the only claim this module can honestly make until issue #26 is done
        Assertions.checkNotNull(Class.forName("org.jgroups.JChannel"));
        Assertions.checkNotBlank(Version.printVersion());
    }

    @Test
    public void testChannelTestHarnessIsOnTheTestClasspath() throws ClassNotFoundException {
        //the pom declares invesdwin-context-integration-channel's test-jar at test scope; this is the harness a real
        //JGroups test extends, and a test-jar coordinate that stops resolving fails here instead of at whoever needs it
        Assertions.checkNotNull(Class.forName("de.invesdwin.context.integration.channel.AChannelTest"));
        Assertions.checkNotNull(Class.forName("de.invesdwin.context.integration.channel.sync.ISynchronousChannel"));
    }

    @Test
    public void testSynchronousChannelAdapterHonoursTheHouseContract() throws ClassNotFoundException {
        Assumptions.assumeTrue(isAdapterImplemented(),
                "no JGroups channel adapter yet (issue #26) — this cell skips rather than passing, so it starts "
                        + "checking the moment " + ADAPTER_CLASS_NAME + " lands");
        final Class<?> adapter = Class.forName(ADAPTER_CLASS_NAME);
        Assertions.assertThat(ISynchronousChannel.class.isAssignableFrom(adapter)).isTrue();
    }

    static boolean isAdapterImplemented() {
        try {
            Class.forName(ADAPTER_CLASS_NAME);
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }

}
