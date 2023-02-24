/*
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2019. ALL RIGHTS RESERVED. See file LICENSE for terms.
 */

package de.invesdwin.context.integration.channel.sync.ucx.jucx.examples;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucs.UcsConstants;

abstract class UcxTest {
    protected static class MemoryBlock implements Closeable {
        private final UcpMemory memory;
        private UcpEndpoint selfEp;
        private ByteBuffer buffer;
        private final UcpWorker worker;

        protected MemoryBlock(final UcpWorker worker, final UcpMemory memory) {
            this.memory = memory;
            this.worker = worker;
            if (memory.getMemType() == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
                this.selfEp = worker.newEndpoint(new UcpEndpointParams().setUcpAddress(worker.getAddress()));
            } else {
                buffer = UcxUtils.getByteBufferView(memory.getAddress(), memory.getLength());
            }
        }

        public UcpMemory getMemory() {
            return memory;
        }

        public void setData(final String data) throws Exception {
            if (memory.getMemType() == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
                final ByteBuffer srcBuffer = ByteBuffer.allocateDirect(data.length() * 2);
                srcBuffer.asCharBuffer().put(data);
                selfEp.sendTaggedNonBlocking(srcBuffer, 0, null);
                worker.progressRequest(
                        worker.recvTaggedNonBlocking(memory.getAddress(), data.length() * 2L, 0, 0, null));
            } else {
                buffer.asCharBuffer().put(data);
            }
        }

        public ByteBuffer getData() throws Exception {
            if (memory.getMemType() == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA) {
                final ByteBuffer dstBuffer = ByteBuffer.allocateDirect((int) memory.getLength());
                selfEp.sendTaggedNonBlocking(memory.getAddress(), memory.getLength(), 0, null);
                worker.progressRequest(worker.recvTaggedNonBlocking(dstBuffer, 0L, 0L, null));
                return dstBuffer;
            } else {
                return buffer;
            }
        }

        @Override
        public void close() {
            memory.close();
            if (selfEp != null) {
                selfEp.close();
            }
        }
    }

    // Stack of closable resources (context, worker, etc.) to be closed at the end.
    protected static Stack<Closeable> resources = new Stack<>();

    protected void closeResources() {
        while (!resources.empty()) {
            try {
                resources.pop().close();
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected static MemoryBlock allocateMemory(final UcpContext context, final UcpWorker worker, final int memType,
            final long length) {
        final UcpMemMapParams memMapParams = new UcpMemMapParams().allocate().setLength(length).setMemoryType(memType);
        return new MemoryBlock(worker, context.memoryMap(memMapParams));
    }
}
