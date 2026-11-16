package de.invesdwin.context.integration.channel.jgroups;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The entry point the module's surefire include has named all along: {@code pom.xml:20} includes
 * {@code de/invesdwin/context/integration/channel/jgroups/JgroupsChannelTestSuite.java}, and until this class was
 * created on 2026-11-15 no such file existed — an include naming a missing path is not an error, so the module reported
 * success while running zero tests. This module has no main sources at all on trunk: the channel itself is the open
 * work tracked as issue #26 ("implement channel for jgroups", jgroups.org, used by Keycloak and Axon Server for
 * clustering).
 * <p>
 * What is selected today is only {@link JgroupsChannelAvailabilityTest}, and a suite that discovers no test is not an
 * option either ({@code NoTestsDiscovered} fails the build), so this class deliberately stays a placeholder with an
 * honest content rather than an empty one.
 * <p>
 * When the channel lands, the family to select here is the one every sibling channel module uses — a
 * {@code JgroupsSynchronousChannel} plus its reader/writer, and the harness subclasses
 * {@code JgroupsChannelTest} / {@code BidiJgroupsChannelTest} / {@code RpcJgroupsChannelTest} extending
 * {@code de.invesdwin.context.integration.channel.AChannelTest} and the latency/throughput harnesses from the channel
 * test-jar, which this module already depends on at test scope. JGroups' own transports need a network decision first:
 * the default stack uses multicast discovery, and the sibling modules that bind a local address use
 * {@code AChannelTest.findLocalNetworkAddress()}, which walks the routable interfaces and returns the first
 * {@code 192.}-prefixed reachable one — see issues #67 and #51 for what that assumption already cost the RDMA modules,
 * so a JGroups test has to pick its protocol stack explicitly (e.g. TCP_NIO2/TCPING with an explicit ping list)
 * instead of inheriting the default.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ JgroupsChannelAvailabilityTest.class })
@Immutable
// CHECKSTYLE:ON
public class JgroupsChannelTestSuite {

}
