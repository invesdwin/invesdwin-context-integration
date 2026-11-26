package de.invesdwin.context.integration.grid.docker;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The docker-daemon-backed half of this module's test surface, kept as a second suite so the module's build entry point
 * {@link DockerContainerTestSuite} stays fast and green on a box with no daemon. The module's surefire block names both
 * suites, so this one does run in a build — its cells simply SKIP where the daemon is not usable, which is recorded as a
 * skip with a reason rather than as a pass.
 * <p>
 * Selected: {@link DockerContainerClaimTest}. Everything it needs it provisions itself: its own free host ports, its own
 * fixture containers (a general-purpose JDK image running a one-line shell fixture that holds a host port and serves
 * nothing, so no image of ours and nothing listening on the host beyond the published port), and a finally-block removal
 * per cell. The first run on a box without that image pulls ~450 MB — that is the cost of not assuming anything
 * installed.
 * <p>
 * Still deliberately outside both suites: {@link DockerProcessBindingProbe}, whose cells contact a real daemon, start
 * containers on fixed host ports and send {@code kill -9} at other processes. It is how the race handling in the
 * mechanism was measured, and it is run by hand.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ DockerContainerClaimTest.class })
@Immutable
// CHECKSTYLE:ON
public class DockerContainerIntegrationTestSuite {

}
