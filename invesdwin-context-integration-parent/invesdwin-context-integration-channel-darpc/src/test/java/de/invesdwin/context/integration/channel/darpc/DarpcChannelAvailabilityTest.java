package de.invesdwin.context.integration.channel.darpc;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * The suite needs a member that a build can always discover, and this is it: it states the precondition the RDMA
 * harnesses of this module depend on, and reports it honestly rather than letting them error.
 * <p>
 * On a machine without the disni native library the cell is skipped by an assumption, not passed - a skip must never
 * look like a pass. Measured on subes-lap-mid, 2026-11-15: no box in this fleet that this session could reach has the
 * library (subes-lap-mid, subes-lappy and ptstaging each report no infiniband device and no libdisni), and the eight
 * latency classes of this module fail with {@code java.lang.UnsatisfiedLinkError: no disni in java.library.path}
 * followed by the permanent {@code NoClassDefFoundError: Could not initialize class
 * com.ibm.disni.verbs.impl.NativeDispatcher}.
 */
@NotThreadSafe
public class DarpcChannelAvailabilityTest extends ATest {

    @Test
    public void testDisniNativeLibraryAvailable() throws ClassNotFoundException {
        Assumptions.assumeTrue(isDisniNativeAvailable(),
                "no disni native library on this box - the RDMA harnesses of this module cannot run");
        //with the library present the verbs dispatcher must initialise: this is the class the eight harnesses die on
        Assertions.checkNotNull(Class.forName("com.ibm.disni.verbs.impl.NativeDispatcher"));
    }

    static boolean isDisniNativeAvailable() {
        try {
            System.loadLibrary("disni");
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

}
