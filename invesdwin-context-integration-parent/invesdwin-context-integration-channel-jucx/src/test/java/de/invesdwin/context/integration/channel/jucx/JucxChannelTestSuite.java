package de.invesdwin.context.integration.channel.jucx;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The build and CI run this suite and nothing else in the module.
 * <p>
 * What is recorded below is measured, not inherited (2026-11-15: one module run plus one run per class, on a box that
 * has the UCX engine and no RDMA link; the load average was 13-22 from an unrelated process, which depresses the rates
 * but not the failure mode). {@link BidiJucxChannelTest} writes and reads all 1,000 records and then fails the close
 * handshake when the read does not return within {@code AChannelTest.MAX_WAIT_DURATION}. {@link JucxChannelTest}
 * completes its 1,000 records too and then does not finish, in one of two shapes, seen in both class orders and with
 * this suite's availability cell absent: run alone it simply hangs, both of its threads at full CPU inside
 * {@code UcpEndpoint.destroyEndpointNative}, reached from the channel finalizer's quiet close, for six measured
 * minutes; run after another UCX class in the reused surefire fork it prints the worker-ownership assertion
 * {@code ucs_async_check_owner_thread(&(worker)->async)} from {@code ucp_ep.c}, surefire records the fork as gone and
 * no {@code Tests run} line for it ever appears - after which the JVM process frequently keeps spinning at full CPU for
 * minutes anyway, so a build that has already reported may still have left a fork running. The mechanism that
 * assertion guards is written down once, in the channel-hadronio suite's javadoc, rather than in two places.
 * {@link RpcJucxChannelTest} is selected and disabled at the class level ("jucx version mismatch"), which the parent
 * pom's {@code version.jucx} against the box's UCX release has never resolved. This module is therefore red before any
 * machine change, and it stays red here: the failure is in the channel's teardown, not in a missing transport, and it
 * is not hidden behind an assumption.
 * <p>
 * {@link JucxChannelAvailabilityTest} states the precondition the RDMA work depends on - that the engine loads, and
 * that no rc_verbs device is present - and reports the missing half by skipping. It is listed
 * first so that its reading exists before the fork can go: this suite does not reach its own end today, and a cell
 * placed last is a cell that never reports. The equivalent cell in the channel-hadronio suite, in a module that does
 * terminate, is the one to watch during a software-RDMA sitting.
 * <p>
 * What a red run of this module costs, measured on one box, with the bound {@code
 * -DforkedProcessTimeoutInSeconds=240} set: it is not reliable. One run reported {@code There was a timeout in the
 * fork} and failed - the fork's report stamp is 16:21:59, the bound is 240 s, and the build still took until 16:27:18,
 * so about 80 s of surefire's own shutdown and wait after the bound was reached. A second run of the same module on the
 * same branch reported nothing for eight minutes: its fork was still at two full CPUs 8m22s after starting, the log had
 * gone quiet in minute two, and 'There was a timeout in the fork' appeared only once that fork was killed by hand. So
 * the bound is worth setting and is not worth trusting - it sometimes ends the run, and sometimes it is only noticed
 * when someone else ends it. Whoever runs this build checks for a leftover {@code ForkedBooter} of the checkout before
 * believing the run is over. Whether the UCX class hangs or aborts is not
 * predictable from the suite's contents: both shapes were seen in both class orders, with and without this cell, and
 * no mechanism is known for the difference. Whoever runs this build checks for a leftover {@code ForkedBooter} before
 * believing the run is over.
 * <p>
 * Deliberately not selected: the ten classes under {@code examples} - seven named {@code *Test.java}, six of which
 * hold cells while {@code UcxTest} is their abstract base, plus three benchmark mains with no cells at all. They are
 * vendored upstream JUCX tests of that binding rather than of this module's channels. Measured per class, one
 * surefire run each:
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpContextTest} 4 cells in 0.2 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpWorkerTest} 8 in 0.9 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpRequestTest} 1 in 0.2 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpMemoryTest} 3 in 0.3 s and
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpListenerTest} 1 in 0.3 s all pass offline, while
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpEndpointTest} never finishes (its first line for
 * testGetNB printed and nothing was reported for over four minutes before the probe harness killed it) - the same
 * endpoint path that hangs this module's own channels. None of them is selected, because a second UCX-using class in
 * one reused fork can end it, which would replace
 * the report of the classes above with a crash rather than add coverage of this module.
 */
@Suite
@SelectClasses({ JucxChannelAvailabilityTest.class, BidiJucxChannelTest.class, JucxChannelTest.class,
        RpcJucxChannelTest.class })
@Immutable
public class JucxChannelTestSuite {

}
