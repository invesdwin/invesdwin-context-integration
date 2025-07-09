package de.invesdwin.context.integration.channel.sync.jucx.examples;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class UcpMemoryTest extends UcxTest {
    public static final int MEM_SIZE = 4096;
    public static final String RANDOM_TEXT = UUID.randomUUID().toString();

    @Test
    public void testMmapFile() throws Exception {
        final UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
        final Path tempFile = Files.createTempFile("jucx", "test");
        // 1. Create FileChannel to file in tmp directory.
        final FileChannel fileChannel = FileChannel.open(tempFile, CREATE, WRITE, READ, DELETE_ON_CLOSE);
        final MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MEM_SIZE);
        buf.asCharBuffer().put(RANDOM_TEXT);
        buf.force();
        // 2. Register mmap buffer with ODP
        final UcpMemory mmapedMemory = context.memoryMap(
                new UcpMemMapParams().setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());

        Assertions.checkEquals(mmapedMemory.getAddress(), UcxUtils.getAddress(buf));

        // 3. Test allocation
        final UcpMemory allocatedMemory = context.memoryMap(new UcpMemMapParams().allocate()
                .setProtection(UcpConstants.UCP_MEM_MAP_PROT_LOCAL_READ)
                .setLength(MEM_SIZE)
                .nonBlocking());
        Assertions.checkEquals(allocatedMemory.getLength(), MEM_SIZE);

        allocatedMemory.deregister();
        mmapedMemory.deregister();
        fileChannel.close();
        context.close();
    }

    @Test
    public void testGetRkey() {
        final UcpContext context = new UcpContext(new UcpParams().requestRmaFeature());
        final java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocateDirect(MEM_SIZE);
        final UcpMemory mem = context.registerMemory(buf);
        final java.nio.ByteBuffer rkeyBuffer = mem.getRemoteKeyBuffer();
        Assertions.checkTrue(rkeyBuffer.capacity() > 0);
        Assertions.checkTrue(mem.getAddress() > 0);
        mem.deregister();
        context.close();
    }

    @Test
    public void testRemoteKeyUnpack() {
        final UcpContext context = new UcpContext(new UcpParams().requestRmaFeature());
        final UcpWorker worker1 = new UcpWorker(context, new UcpWorkerParams());
        final UcpWorker worker2 = new UcpWorker(context, new UcpWorkerParams());
        final UcpEndpoint endpoint = new UcpEndpoint(worker1,
                new UcpEndpointParams().setUcpAddress(worker2.getAddress()));
        final java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocateDirect(MEM_SIZE);
        final UcpMemory mem = context.registerMemory(buf);
        final UcpRemoteKey rkey = endpoint.unpackRemoteKey(mem.getRemoteKeyBuffer());
        Assertions.checkNotNull(rkey.getNativeId());

        Collections.addAll(resources, context, worker1, worker2, endpoint, mem, rkey);
        closeResources();
    }
}
