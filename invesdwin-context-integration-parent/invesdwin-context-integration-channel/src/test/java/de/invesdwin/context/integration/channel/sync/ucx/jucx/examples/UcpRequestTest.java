// @NotThreadSafe
// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.ucx.jucx.examples;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

public class UcpRequestTest {
    @Test
    public void testCancelRequest() throws Exception {
        final UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
        final UcpWorker worker = context.newWorker(new UcpWorkerParams());
        final UcpRequest recv = worker.recvTaggedNonBlocking(ByteBuffer.allocateDirect(100), null);
        worker.cancelRequest(recv);

        while (!recv.isCompleted()) {
            worker.progress();
        }

        Assertions.assertEquals(UcsConstants.STATUS.UCS_ERR_CANCELED, recv.getStatus());
        Assertions.assertTrue(recv.isCompleted());
        Assertions.assertNull(recv.getNativeId());

        worker.close();
        context.close();
    }
}
