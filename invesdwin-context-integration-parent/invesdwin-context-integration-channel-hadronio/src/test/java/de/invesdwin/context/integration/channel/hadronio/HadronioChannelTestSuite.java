package de.invesdwin.context.integration.channel.hadronio;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.hadronio.blocking.BidiBlockingHadronioSocketChannelTest;
import de.invesdwin.context.integration.channel.hadronio.blocking.BlockingHadronioSocketChannelTest;

/**
 * The build and CI run this suite and nothing else in the module.
 * <p>
 * hadroNIO has one provider only - {@code de.hhu.bsinfo.hadronio.jucx.JucxProvider}, its Configuration's default
 * provider class and the only provider class the artifact ships - so every channel here rides UCX, and
 * {@link HadronioChannelAvailabilityTest} states what that means for a build: hadroNIO's provider is constructed and
 * the RDMA half is skipped until an rxe or siw link exists. Measured 2026-11-15 (one module run, plus one run per
 * class; load average 13-22 from an unrelated process), the three socket cells below each run their 1,000 records and
 * report in 0.4 to 2.7 s, which says nothing about the transport: with no RDMA device configured UCX offers only
 * shared-memory and tcp transports here.
 * <p>
 * {@link HadronioSocketChannelTest} is selected and disabled at the class level ("TODO: might hang/crash sometimes");
 * a disabled class is discovered but contributes no cell to the tally.
 * <p>
 * Deliberately not selected, with the reason measured rather than inherited: the three netty variants
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.HadronioNettySocketChannelTest},
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.BidiHadronioNettySocketChannelTest} and
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.HadronioNettySocketHandlerTest} each abort the
 * surefire fork before reporting anything - exit 134, {@code ucp_worker.c:3067}
 * {@code Assertion 'ucs_async_check_owner_thread(&(worker)->async)' failed} - one run per class, the same UCX
 * worker-ownership violation that aborts the jucx module. Those three line numbers and that exit code come from the
 * single-class probe runs, not from a run of this suite; the jucx module's own abort is what this suite's runs show.
 * <p>
 * The mechanism to investigate is the worker's thread-safety request, not the context's MT mode. hadroNIO's worker is
 * created with {@code UcpWorkerParams} that ask only for {@code requestWakeupTagRecv} and
 * {@code requestWakeupTagSend} (javap of hadronio-0.3.3: those are the only {@code UcpWorkerParams} calls in the
 * artifact, and {@code requestThreadSafety} appears nowhere in it), and this repository's own jucx channel passes a
 * bare {@code new UcpWorkerParams()} ({@code JucxSynchronousChannel.newUcpWorkerParams()}, sibling module). The only
 * three {@code requestThreadSafety()} call sites in that module are the vendored upstream examples that do not crash:
 * {@code examples/UcpWorkerTest.java:48} and {@code :74} and {@code examples/UcpEndpointTest.java:237}, each paired
 * with a multi-worker or MT-shared context. That is where the difference is visible inside this tree; whether it is
 * the whole story is untested, so it is a lead and not a claim. What is already refuted: {@code setMtWorkersShared} -
 * the two modules set it to opposite values (false in {@code JucxSynchronousChannel}, true in hadroNIO's provider) and
 * both abort. So the work that unblocks the netty variants is that question, or a UCX build without
 * {@code --enable-mt} - not an RDMA device: until it is settled they would replace the module's report with a crash
 * instead of adding coverage.
 */
@Suite
@SelectClasses({ HadronioChannelAvailabilityTest.class, BidiBlockingHadronioSocketChannelTest.class,
        BlockingHadronioSocketChannelTest.class, BidiHadronioSocketChannelTest.class, HadronioSocketChannelTest.class })
@Immutable
public class HadronioChannelTestSuite {

}
