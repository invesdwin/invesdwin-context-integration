package de.invesdwin.context.integration.channel.jucx;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The build and CI run this suite and nothing else in the module.
 * <p>
 * What is below is measured, not inherited (2026-11-15: one module run plus one run per class, on a box that has the
 * UCX engine and no RDMA link; the load average was 13-22 from an unrelated process, which depresses the rates but not
 * the failure mode). {@link BidiJucxChannelTest} writes and reads all 1,000 records and then fails the close handshake
 * when the read does not return within {@code AChannelTest.MAX_WAIT_DURATION}. {@link JucxChannelTest} completes its
 * 1,000 records too and then does not finish: run alone, both of its threads sit at full CPU inside
 * {@code UcpEndpoint.destroyEndpointNative}, reached from the channel finalizer's quiet close, for minutes; run after
 * another UCX class in the same reused surefire fork, it aborts the JVM instead on the worker-ownership assertion
 * {@code ucs_async_check_owner_thread(&(worker)->async)} in {@code ucp_ep.c}, so no {@code Tests run} line for it ever
 * appears. {@link RpcJucxChannelTest} is selected and disabled at the class level ("jucx version mismatch"), which the
 * parent pom's {@code version.jucx} against the box's UCX release has never resolved. This module is therefore red
 * before any machine change, and it stays red here: the failure is in the channel's teardown, not in a missing
 * transport, and it is not hidden behind an assumption.
 * <p>
 * {@link JucxChannelAvailabilityTest} states the precondition the RDMA work depends on - the engine loads, and no
 * rc_verbs device is present - and reports the missing half by skipping. It is listed last because that is where it
 * costs nothing: measured, with that cell run first the next UCX class hangs rather than aborts, which would stretch
 * this module's red run out to the bom's 1,800 s fork timeout. While the fork dies early, as it does today, the cell
 * does not get to report here; the equivalent cell in the channel-hadronio suite does, and that is the one to watch
 * during a software-RDMA sitting.
 * <p>
 * Deliberately not selected: the seven classes under {@code examples}, which are vendored upstream JUCX tests of that
 * binding rather than of this module's channels. Measured per class, one surefire run each:
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpContextTest} 4 cells in 0.2 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpWorkerTest} 8 in 0.9 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpRequestTest} 1 in 0.2 s,
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpMemoryTest} 3 in 0.3 s and
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpListenerTest} 1 in 0.3 s all pass offline, while
 * {@link de.invesdwin.context.integration.channel.jucx.examples.UcpEndpointTest} never finishes (its first line for
 * testGetNB printed and nothing was reported for over four minutes before the probe harness killed it) - the same
 * endpoint path that hangs this module's own channels. {@code UcxTest} is their abstract base and holds no test of its
 * own. None of them is selected, because a second UCX-using class in one reused fork can end it, which would replace
 * the report of the classes above with a crash rather than add coverage of this module.
 */
@Suite
@SelectClasses({ BidiJucxChannelTest.class, JucxChannelTest.class, RpcJucxChannelTest.class,
        JucxChannelAvailabilityTest.class })
@Immutable
public class JucxChannelTestSuite {

}
