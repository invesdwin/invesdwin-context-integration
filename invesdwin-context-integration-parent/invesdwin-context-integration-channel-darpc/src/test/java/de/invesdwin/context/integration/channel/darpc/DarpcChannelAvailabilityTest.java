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
 * look like a pass. Measured 2026-11-15 over ssh on all three dev boxes: none of them has an infiniband device,
 * no libdisni in ldconfig and no libdisni on disk, while the disni 2.1 Java artifact sits in
 * every local repository - the module compiles and only the native load fails - and the eight latency classes all die on {@code java.lang.UnsatisfiedLinkError: no disni in java.library.path}
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
