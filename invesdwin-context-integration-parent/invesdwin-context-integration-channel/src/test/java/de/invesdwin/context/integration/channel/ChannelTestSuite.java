package de.invesdwin.context.integration.channel;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.rpc.RpcChannelTestSuite;
import de.invesdwin.context.integration.channel.stream.StreamChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.aeron.AeronChannelTest;
import de.invesdwin.context.integration.channel.sync.agrona.AgronaChannelTest;
import de.invesdwin.context.integration.channel.sync.bufferingiterator.BufferingIteratorChannelTest;
import de.invesdwin.context.integration.channel.sync.chronicle.ChronicleChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.compression.CompressionChannelTest;
import de.invesdwin.context.integration.channel.sync.compression.stream.StreamCompressionChannelTest;
import de.invesdwin.context.integration.channel.sync.conversant.ConversantChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.CryptoChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.fragment.FragmentChannelTest;
import de.invesdwin.context.integration.channel.sync.jctools.JctoolsChannelTest;
import de.invesdwin.context.integration.channel.sync.kryonet.KryonetChannelTest;
import de.invesdwin.context.integration.channel.sync.lmax.LmaxChannelTest;
import de.invesdwin.context.integration.channel.sync.mina.MinaChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.netty.NettyChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.pipe.PipeChannelTest;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeChannelTest;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeChannelTest;
import de.invesdwin.context.integration.channel.sync.queue.QueueChannelTest;
import de.invesdwin.context.integration.channel.sync.reference.ReferenceChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.SocketChannelTestSuite;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.TimeSeriesDBChannelTestSuite;

@Suite
@SelectClasses({ AeronChannelTest.class, AgronaChannelTest.class, BufferingIteratorChannelTest.class,
        ConversantChannelTest.class, JctoolsChannelTest.class, KryonetChannelTest.class, LmaxChannelTest.class,
        CompressionChannelTest.class, PipeChannelTest.class, StreamingPipeChannelTest.class,
        NativePipeChannelTest.class, QueueChannelTest.class, ReferenceChannelTest.class, FragmentChannelTest.class,
        CompressionChannelTest.class, StreamCompressionChannelTest.class, CryptoChannelTestSuite.class,
        SocketChannelTestSuite.class, NettyChannelTestSuite.class, MinaChannelTestSuite.class,
        ChronicleChannelTestSuite.class, TimeSeriesDBChannelTestSuite.class, StreamChannelTestSuite.class,
        RpcChannelTestSuite.class })
@Immutable
public class ChannelTestSuite {

}
