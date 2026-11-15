package de.invesdwin.context.integration.channel.hadronio;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * States the precondition the RDMA work on this module depends on, inside the build, so that a run which never touched
 * an RDMA transport cannot be read as one.
 * <p>
 * hadroNIO has exactly one provider: {@code de.hhu.bsinfo.hadronio.jucx.JucxProvider}, which is both its
 * Configuration's default provider class and the only provider class the artifact contains, so every channel here
 * rides UCX and the operating system's transport list - the hadroNIO artifact declares no dependencies of its own,
 * which is why this module's pom adds the jucx one itself. Measured 2026-11-15 on a dev box with no RDMA link
 * configured, a UCX context asking for TLS "rc_verbs" or "ud_verbs" fails with {@code UcxException: No such device}
 * while "all", "tcp" and "self,cma,sysv" each succeed over cma, posix, self, sysv and tcp. The cells of this suite
 * that do run are therefore running over a shared-memory or tcp transport, and this cell is the one that stays skipped
 * until an rxe or siw link exists.
 */
@NotThreadSafe
public class HadronioChannelAvailabilityTest extends ATest {

    @Test
    public void testUcpEngineAvailable() {
        final UcpContext context = newUcpContext(null);
        Assumptions.assumeTrue(context != null,
                "no UCX engine on this box - hadroNIO's only provider is the jucx one, whose jar ships only JNI glue and links the system libucp, libuct, libucs and libucm");
        try {
            Assertions.checkTrue(context.getNativeId() > 0);
        } finally {
            context.close();
        }
    }

    @Test
    public void testUcpRdmaTransportAvailable() {
        final UcpContext context = newUcpContext("rc_verbs");
        Assumptions.assumeTrue(context != null,
                "UCX sees no rc_verbs transport on this box - no rxe or siw link is configured, so hadroNIO runs over a shared-memory or tcp transport only");
        try {
            Assertions.checkTrue(context.getNativeId() > 0);
        } finally {
            context.close();
        }
    }

    /**
     * @return the context, or null if the engine is not loadable or the transport is not present - both are preconditions
     *         this cell reports by skipping, and neither is a verdict about the channel implementation.
     */
    private static UcpContext newUcpContext(final String tls) {
        final UcpParams params = new UcpParams().requestTagFeature();
        if (tls != null) {
            params.setConfig("TLS", tls);
        }
        try {
            return new UcpContext(params);
        } catch (final UcxException | UnsatisfiedLinkError e) {
            return null;
        }
    }

}
