package de.invesdwin.context.integration.channel.darpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The build and CI run this suite and nothing else in the module. It is the entry point the module's surefire include
 * has named all along although the class never existed, which is why the module reported success while running zero
 * tests and no build could say so.
 * <p>
 * Only the availability cell is selected, because the module's eight latency harnesses over the RDMA channels cannot
 * run in a build as they stand: on a machine without the disni native library they all error with
 * {@code java.lang.UnsatisfiedLinkError: no disni in java.library.path}, followed by the permanent
 * {@code NoClassDefFoundError: Could not initialize class com.ibm.disni.verbs.impl.NativeDispatcher}. Measured per
 * class on one dev box, 2026-11-15, one surefire run each: 11 errors across the eight -
 * {@link DarpcChannelAvailabilityTest} states that precondition instead of hiding it, since an empty suite is not
 * allowed either ({@code NoTestsDiscovered} fails the build).
 * <p>
 * When the RDMA transport work lands, the classes to add here are
 * {@link de.invesdwin.context.integration.channel.darpc.rpc.client.DarpcClientChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.rpc.client.BidiDarpcClientChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.rpc.disni.RpcSynchronousDisniActiveChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.rpc.disni.RpcSynchronousDisniPassiveChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.sync.active.DisniActiveChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.sync.active.BidiDisniActiveChannelTest},
 * {@link de.invesdwin.context.integration.channel.darpc.sync.passive.DisniPassiveChannelTest} and
 * {@link de.invesdwin.context.integration.channel.darpc.sync.passive.BidiDisniPassiveChannelTest}. All eight now bind
 * a discovered local address ({@code findLocalNetworkAddress()}), so the native library is the only precondition left;
 * the two rpc.client ones used to bind a fixed host address that no box in the fleet can bind at all.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ DarpcChannelAvailabilityTest.class })
@Immutable
// CHECKSTYLE:ON
public class DarpcChannelTestSuite {

}
