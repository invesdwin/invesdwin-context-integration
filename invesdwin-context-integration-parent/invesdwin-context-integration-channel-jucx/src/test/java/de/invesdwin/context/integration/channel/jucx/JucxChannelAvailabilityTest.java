package de.invesdwin.context.integration.channel.jucx;

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
 * The jucx artifact ships only its JNI glue: its jar contains exactly one native (libjucx.so), that library's NEEDED
 * entries are the system libucm/libucs/libuct/libucp (plus libnuma), and its loader org.openucx.jucx.NativeLibs asks
 * for those four as jar resources first and falls back to {@code System.loadLibrary}. So the UCX engine in play is the
 * operating system's, and which transports it enumerates decides what a green channel test in this module means.
 * Measured 2026-11-15 on a dev box with no RDMA link configured: TLS "all", "tcp" and "self,cma,sysv" each create a
 * context (over cma, posix, self, sysv and tcp), while "rc_verbs" and "ud_verbs" both fail with
 * {@code UcxException: No such device} while UCX names that same usable set. A channel test passing is therefore not
 * evidence of an RDMA run; this cell is the one that stays skipped until an rxe or siw link exists, and it turns green
 * as the positive control when one does.
 */
@NotThreadSafe
public class JucxChannelAvailabilityTest extends ATest {

    @Test
    public void testUcpEngineAvailable() {
        final UcpContext context = newUcpContext(null);
        Assumptions.assumeTrue(context != null,
                "no UCX engine on this box - the jucx jar ships only its JNI glue and links the system libucp, libuct, libucs and libucm");
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
                "UCX sees no rc_verbs transport on this box - no rxe or siw link is configured, so the channels of this module run over a shared-memory or tcp transport only");
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
