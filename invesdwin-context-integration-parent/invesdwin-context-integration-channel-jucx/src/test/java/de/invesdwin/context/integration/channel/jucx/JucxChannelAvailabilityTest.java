package de.invesdwin.context.integration.channel.jucx;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.NativeLibs;
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
 * entries are the system libucm/libucs/libuct/libucp (plus libnuma), and its loader {@code org.openucx.jucx.NativeLibs}
 * asks for those four as jar resources first and falls back to {@code System.loadLibrary}. So the UCX engine in play is
 * the operating system's, and which transports it enumerates decides what a green channel test in this module means.
 * Measured 2026-11-15 on a dev box with no RDMA link configured: TLS "all", "tcp" and "self,cma,sysv" each create a
 * context (over cma, posix, self, sysv and tcp), while "rc_verbs" and "ud_verbs" both fail with
 * {@code UcxException: No such device} while UCX names that same usable set. A channel test passing is therefore not
 * evidence of an RDMA run; the transport cell is the one that stays skipped until an rxe or siw link exists, and it
 * turns green as the positive control when one does.
 * <p>
 * Both cells skip and neither errors, and the shapes here are what make that hold. {@link UcpContext} has a static
 * initializer that calls {@code NativeLibs.load()} (javap of the shipped jar: {@code UcpContext.<clinit>} →
 * {@code NativeLibs.load:()V}), so an unloadable engine reaches a caller as {@code ExceptionInInitializerError} the
 * first time and as {@code NoClassDefFoundError: Could not initialize class org.openucx.jucx.ucp.UcpContext} every
 * time afterwards - a permanently burned class in the one reused surefire fork this build runs in. That is why the
 * engine cell asks {@code NativeLibs.load()} directly and never touches {@code UcpContext}, and why the transport
 * cell catches {@link LinkageError} rather than {@code UnsatisfiedLinkError}. Same shape as the burned
 * {@code com.ibm.disni.verbs.impl.NativeDispatcher} recorded in this repository's darpc availability cell.
 * <p>
 * Neither cell creates a {@code UcpWorker} on purpose. The worker is where this build's UCX aborts the fork (the
 * worker-ownership assertion documented in {@link JucxChannelTestSuite}), and an availability cell that kills the
 * fork would remove the report of every class after it instead of adding a reading.
 */
@NotThreadSafe
public class JucxChannelAvailabilityTest extends ATest {

    @Test
    public void testUcpEngineAvailable() {
        Assumptions.assumeTrue(isUcpEngineLoadable(),
                "no UCX engine on this box - the jucx jar ships only its JNI glue and links the system libucp, libuct, libucs and libucm");
        //the assumption above is what keeps this safe: once the libraries are loaded, UcpContext's initializer cannot fail
        final UcpContext context = newChannelUcpContext(null);
        Assumptions.assumeTrue(context != null,
                "the UCX libraries load but no context can be created, so no transport set on this box satisfies the parameters the channels ask with");
        try {
            Assertions.checkTrue(context.getNativeId() > 0);
        } finally {
            context.close();
        }
    }

    @Test
    public void testUcpRdmaTransportAvailable() {
        final UcpContext context = newChannelUcpContext("rc_verbs");
        Assumptions.assumeTrue(context != null,
                "UCX sees no rc_verbs transport on this box - no rxe or siw link is configured, so the channels of this module run over a shared-memory or tcp transport only");
        try {
            Assertions.checkTrue(context.getNativeId() > 0);
        } finally {
            context.close();
        }
    }

    /**
     * Probes the engine without going through {@code UcpContext}, whose static initializer loads the natives and whose
     * failed initialization cannot be revived in this JVM.
     *
     * @return true if the UCX libraries the binding links are loadable.
     */
    static boolean isUcpEngineLoadable() {
        try {
            NativeLibs.load();
            return true;
        } catch (final LinkageError e) {
            return false;
        }
    }

    /**
     * Asks for a context with the parameters the channel of this module asks with, so that the reading is about the
     * transport and not about a parameter the production path never uses:
     * {@code JucxSynchronousChannel.newUcpContextParams()} plus the {@code DEFAULT} type's own contribution - and
     * {@code JucxTransportType.DEFAULT} is {@code STREAM}, whose {@code configureContextParams} adds
     * {@code requestStreamFeature()}.
     *
     * @return the context, or null if the engine is not loadable or the transport is not present. Both are preconditions
     *         this cell reports by skipping, and neither is a verdict about the channel implementation.
     */
    private static UcpContext newChannelUcpContext(final String tls) {
        final UcpParams params = new UcpParams().requestWakeupFeature().setEstimatedNumEps(1)
                .setMtWorkersShared(false).requestStreamFeature();
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
