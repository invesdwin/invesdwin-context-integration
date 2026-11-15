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
 * {@link HadronioChannelAvailabilityTest} states what that means for a build: the engine loads, and the RDMA half is
 * skipped until an rxe or siw link exists. Measured 2026-11-15 (one module run, plus one run per class; load average
 * 13-22 from an unrelated process), the three socket cells below each run their 1,000 records and report in 0.4 to
 * 2.7 s, which says nothing about the transport: with no RDMA device configured UCX offers only shared-memory and tcp
 * transports here.
 * <p>
 * {@link HadronioSocketChannelTest} is selected and disabled at the class level ("TODO: might hang/crash sometimes");
 * a disabled class is discovered but contributes no cell to the tally.
 * <p>
 * Deliberately not selected, with the reason measured rather than inherited: the three netty variants
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.HadronioNettySocketChannelTest},
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.BidiHadronioNettySocketChannelTest} and
 * {@link de.invesdwin.context.integration.channel.hadronio.netty.HadronioNettySocketHandlerTest} each abort the
 * surefire fork before reporting anything - exit 134, {@code ucp_worker.c:3067}
 * {@code Assertion 'ucs_async_check_owner_thread(&(worker)->async)' failed} - one run per class, same UCX
 * worker-ownership violation that aborts the jucx module. Their channel type is this module's own
 * {@code HadroNioNettySocketChannelType}, so the work that unblocks them is the UCX thread-mode question
 * ({@code UcpParams.setMtWorkersShared}, or a UCX build without {@code --enable-mt}), not an RDMA device: until that
 * is settled they would replace the module's report with a crash instead of adding coverage.
 */
// CHECKSTYLE:OFF
@Suite
@SelectClasses({ HadronioChannelAvailabilityTest.class, BidiBlockingHadronioSocketChannelTest.class,
        BlockingHadronioSocketChannelTest.class, BidiHadronioSocketChannelTest.class, HadronioSocketChannelTest.class })
@Immutable
// CHECKSTYLE:ON
public class HadronioChannelTestSuite {

}
