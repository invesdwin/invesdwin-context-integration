package de.invesdwin.context.integration.grid.docker;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The build and CI run this suite and nothing else in the module. The module had no surefire configuration at all until
 * it was added on 2026-11-15 together with this class, so surefire's default name patterns decided alone: the module's
 * only test source is {@link DockerProcessBindingProbe}, which none of them match, so the module reported success while
 * running zero tests - which no build could say.
 * <p>
 * Every selected class runs offline: the docker engine API is served by a recording {@code DockerClient} proxy, so the
 * claims, the sweep order and the race handling are asserted without a docker daemon. What that deliberately does not
 * cover is a real daemon accepting the labeled stop/remove and the fixed port re-bind - that needs a container actually
 * started, and is left to the integration run of whoever uses these classes, not claimed here.
 * <p>
 * Deliberately not selected: {@link DockerProcessBindingProbe}. Its four cells contact a real daemon, start containers
 * on fixed host ports and send {@code kill -9} at other processes, so they belong in nobody's build; they are the
 * measurement the race handling above is written against, and they are run by hand.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ ContainerOwnershipTest.class, ContainerReclaimerTest.class, ContainerStarterTest.class })
@Immutable
// CHECKSTYLE:ON
public class DockerContainerTestSuite {

}
