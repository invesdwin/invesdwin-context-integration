package de.invesdwin.context.integration.channel.hadronio;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.NativeLibs;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

import de.hhu.bsinfo.hadronio.jucx.JucxProvider;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * States the precondition the RDMA work on this module depends on, inside the build, so that a run which never touched
 * an RDMA transport cannot be read as one.
 * <p>
 * hadroNIO has exactly one provider: {@code de.hhu.bsinfo.hadronio.jucx.JucxProvider}, which is both its
 * Configuration's default provider class and the only provider class the artifact contains, so every channel here
 * rides UCX and the operating system's transport list - the hadroNIO artifact declares no dependencies of its own,
 * which is why this module's pom adds the jucx one itself. That provider is what the first cell asks, because it is
 * the object this module's channels actually construct; the second cell asks the transport question the sitting is
 * about. Together they say: the engine and hadroNIO's provider are here (or this build is not answering the question
 * at all), and no RDMA transport is.
 * <p>
 * The two are not the same parameters, and that is worth recording next to the readings: the provider's constructor
 * (javap of hadronio-0.3.3) builds its context with {@code requestWakeupFeature().requestTagFeature()
 * .requestStreamFeature().setMtWorkersShared(true)}, while this repository's own jucx channel asks
 * {@code setMtWorkersShared(false)} ({@code JucxSynchronousChannel.newUcpContextParams()} in the sibling module). The
 * hadroNIO layer therefore runs UCX in a different multi-threading shape than the raw jucx channels do, which is one
 * reason their aborts are not interchangeable evidence.
 * <p>
 * Measured 2026-11-15 on a dev box with no RDMA link configured, a UCX context asking for TLS "rc_verbs" or "ud_verbs"
 * fails with {@code UcxException: No such device} while "all", "tcp" and "self,cma,sysv" each succeed over cma, posix,
 * self, sysv and tcp. The cells of this suite that do run are therefore running over a shared-memory or tcp transport,
 * and the transport cell is the one that stays skipped until an rxe or siw link exists.
 * <p>
 * A missing engine skips; a provider that cannot be built once the engine has loaded is an error, because that is a
 * defect in this module's own precondition and not a missing machine feature. The engine is probed through
 * {@code NativeLibs.load()} before anything touches {@code UcpContext}: {@code UcpContext} has a static initializer
 * that calls it (javap: {@code UcpContext.<clinit>} → {@code NativeLibs.load:()V}), so a load failure there becomes
 * {@code ExceptionInInitializerError} and then a permanently burned class in the one reused surefire fork - the same
 * shape as the burned {@code com.ibm.disni.verbs.impl.NativeDispatcher} in this repository's darpc availability cell.
 * No worker is created here either: the worker is where this build's UCX aborts the fork (see
 * {@link HadronioChannelTestSuite}), and a cell that killed the fork would delete the report of every class after it.
 */
@NotThreadSafe
public class HadronioChannelAvailabilityTest extends ATest {

    @Test
    public void testHadronioProviderAvailable() throws IOException {
        Assumptions.assumeTrue(isUcpEngineLoadable(),
                "no UCX engine on this box - hadroNIO's only provider is the jucx one, whose jar ships only JNI glue and links the system libucp, libuct, libucs and libucm");
        try (JucxProvider provider = new JucxProvider()) {
            //the provider is the object the channels of this module build; a listener needs a worker, so this stops here
            Assertions.checkNotNull(provider);
        }
    }

    @Test
    public void testUcpRdmaTransportAvailable() {
        final UcpContext context = newProviderUcpContext("rc_verbs");
        Assumptions.assumeTrue(context != null,
                "UCX sees no rc_verbs transport on this box - no rxe or siw link is configured, so hadroNIO runs over a shared-memory or tcp transport only");
        try {
            Assertions.checkTrue(context.getNativeId() > 0);
        } finally {
            context.close();
        }
    }

    static boolean isUcpEngineLoadable() {
        try {
            NativeLibs.load();
            return true;
        } catch (final LinkageError e) {
            return false;
        }
    }

    /**
     * @return a context built the way hadroNIO's own provider builds one, or null if the transport is not present - a
     *         precondition this cell reports by skipping, not a verdict about the channel implementation.
     */
    private static UcpContext newProviderUcpContext(final String tls) {
        final UcpParams params = new UcpParams().requestWakeupFeature().requestTagFeature().requestStreamFeature()
                .setMtWorkersShared(true);
        if (tls != null) {
            params.setConfig("TLS", tls);
        }
        try {
            return new UcpContext(params);
        } catch (final UcxException | LinkageError e) {
            return null;
        }
    }

}
