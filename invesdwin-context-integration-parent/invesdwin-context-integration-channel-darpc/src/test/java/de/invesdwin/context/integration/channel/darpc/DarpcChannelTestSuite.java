package de.invesdwin.context.integration.channel.darpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The build and CI run this suite and nothing else in the module. It is the entry point the module's surefire include
 * has named all along although the class did not exist until it was created on 2026-11-15, which is why the module
 * reported success while running zero tests and no build could say so.
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
 * {@link de.invesdwin.context.integration.channel.darpc.sync.passive.BidiDisniPassiveChannelTest}. The two rpc.client
 * ones used to bind a fixed host address that no box can bind at all, and now use
 * {@code AChannelTest.findLocalNetworkAddress()} like every other channel test in this repository does.
 * <p>
 * That helper is not transport-agnostic and this suite says so rather than pretending otherwise: it walks
 * {@code NetworkUtil.getLocalAddresses()} and returns the first address starting {@code 192.}, throwing
 * {@code IllegalStateException: no suitable network interface found} if there is none, and that walk drops loopback and
 * IPv6 and probes each remaining address for reachability. On a box whose routable address is a 10.x or a
 * {@code 100.64/10} overlay address the two rpc.client tests therefore fail in setup instead of at bind. That is
 * accepted deliberately: a software-RDMA link has to be created on the netdev the test binds, so a run that picked a
 * non-RDMA interface would not be an RDMA run, and the alternative - inventing a per-module address property - is the
 * kind of rollout machinery this repository does not add. What the change buys is that the address is no longer a
 * constant nobody can bind; the reachability probe is a cost of the house helper and is named here so a future reader
 * does not rediscover it.
 * <p>
 * The address half is not observable in a build yet, and a run of this module must not be read as testing it:
 * {@code DarpcClientSynchronousChannel.open()} creates the disni endpoint group before it binds or connects
 * (endpoint group at :130, {@code bind} at :139, {@code connect} at :150), so without libdisni every one of the eight
 * dies at the native load and never reaches an address. Before and after the address change, the eight fail at the same
 * line with the same root cause, and the reports are not identical: whichever class first touches the verbs dispatcher
 * in the reused fork logs {@code UnsatisfiedLinkError: no disni in java.library.path} and every later one logs the
 * burned {@code NoClassDefFoundError}, so one run showed 17 of the second against 3 of the first and the other 13
 * against 7.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ DarpcChannelAvailabilityTest.class })
@Immutable
// CHECKSTYLE:ON
public class DarpcChannelTestSuite {

}
