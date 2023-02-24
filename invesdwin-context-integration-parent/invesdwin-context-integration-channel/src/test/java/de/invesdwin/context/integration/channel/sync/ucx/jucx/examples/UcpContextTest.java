// @NotThreadSafe
// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.ucx.jucx.examples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucs.UcsConstants;

public class UcpContextTest {

    public static UcpContext createContext(final UcpParams contextParams) {
        final UcpContext context = new UcpContext(contextParams);
        Assertions.assertTrue(context.getNativeId() > 0);
        Assertions.assertTrue(UcsConstants.MEMORY_TYPE.isMemTypeSupported(context.getMemoryTypesMask(),
                UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST));
        return context;
    }

    public static void closeContext(final UcpContext context) {
        context.close();
        Assertions.assertNull(context.getNativeId());
    }

    @Test
    public void testCreateSimpleUcpContext() {
        final UcpParams contextParams = new UcpParams().requestTagFeature().requestAmFeature();
        final UcpContext context = createContext(contextParams);
        closeContext(context);
    }

    @Test
    public void testCreateUcpContextRdma() {
        final UcpParams contextParams = new UcpParams().requestTagFeature()
                .requestRmaFeature()
                .setEstimatedNumEps(10)
                .setMtWorkersShared(false)
                .setTagSenderMask(0L);
        final UcpContext context = createContext(contextParams);
        closeContext(context);
    }

    @Test
    public void testConfigMap() {
        UcpParams contextParams = new UcpParams().requestTagFeature()
                .setConfig("TLS", "abcd")
                .setConfig("NOT_EXISTING_", "234");
        boolean catched = false;
        try {
            createContext(contextParams);
        } catch (final UcxException exception) {
            Assertions.assertEquals("No such device", exception.getMessage());
            catched = true;
        }
        Assertions.assertTrue(catched);

        // Return back original config
        contextParams = new UcpParams().requestTagFeature().setConfig("TLS", "all");
        final UcpContext context = createContext(contextParams);
        closeContext(context);
    }

    @Test
    public void testCatchJVMSignal() {
        Assertions.assertThrows(NullPointerException.class, () -> {
            final UcpParams contextParams = new UcpParams().requestTagFeature();
            final UcpContext context = createContext(contextParams);
            closeContext(context);
            long nullPointer = context.getNativeId();
            nullPointer += 2;
        });
    }
}
