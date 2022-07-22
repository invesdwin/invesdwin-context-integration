package de.invesdwin.context.integration.channel;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.aeron.AeronChannelTest;
import de.invesdwin.context.integration.channel.sync.agrona.AgronaChannelTest;
import de.invesdwin.context.integration.channel.sync.bufferingiterator.BufferingIteratorChannelTest;
import de.invesdwin.context.integration.channel.sync.chronicle.queue.ChronicleQueueChannelTest;
import de.invesdwin.context.integration.channel.sync.compression.CompressionChannelTest;
import de.invesdwin.context.integration.channel.sync.compression.stream.StreamCompressionChannelTest;
import de.invesdwin.context.integration.channel.sync.conversant.ConversantChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.authentication.AuthenticationChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication.AuthenticatedEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication.stream.StreamAuthenticatedEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.stream.StreamEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.fragment.FragmentChannelTest;
import de.invesdwin.context.integration.channel.sync.jctools.JctoolsChannelTest;
import de.invesdwin.context.integration.channel.sync.kryonet.KryonetChannelTest;
import de.invesdwin.context.integration.channel.sync.lmax.LmaxChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.NettyQueueChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.unsafe.NettyNativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.pipe.PipeChannelTest;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeChannelTest;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeChannelTest;
import de.invesdwin.context.integration.channel.sync.queue.QueueChannelTest;
import de.invesdwin.context.integration.channel.sync.reference.ReferenceChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ AeronChannelTest.class, AgronaChannelTest.class, BufferingIteratorChannelTest.class,
        ChronicleQueueChannelTest.class, ConversantChannelTest.class, JctoolsChannelTest.class,
        KryonetChannelTest.class, LmaxChannelTest.class, CompressionChannelTest.class, NettySocketChannelTest.class,
        NettyNativeSocketChannelTest.class, NettyDatagramChannelTest.class, NettyNativeDatagramChannelTest.class,
        NettyQueueChannelTest.class, PipeChannelTest.class, StreamingPipeChannelTest.class, NativePipeChannelTest.class,
        QueueChannelTest.class, ReferenceChannelTest.class, SocketChannelTest.class, BlockingSocketChannelTest.class,
        NativeSocketChannelTest.class, FragmentChannelTest.class, CompressionChannelTest.class,
        StreamCompressionChannelTest.class, EncryptionChannelTest.class, StreamEncryptionChannelTest.class,
        AuthenticationChannelTest.class, AuthenticatedEncryptionChannelTest.class,
        StreamAuthenticatedEncryptionChannelTest.class })
@Immutable
public class ChannelTestSuite {
    //CHECKSTYLE:ON

}
