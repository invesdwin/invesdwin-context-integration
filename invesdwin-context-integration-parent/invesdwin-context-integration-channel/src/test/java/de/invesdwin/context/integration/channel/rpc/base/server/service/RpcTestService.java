package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.Immutable;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.report.ILatencyReport;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.ImmutableFuture;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class RpcTestService implements IRpcTestService, Closeable {

    /*
     * Use only 1 thread to make the future response actually get delayed and not return isDone=true immediately.
     * Simulating a scarce resource.
     */
    private static final WrappedExecutorService ASYNC_EXECUTOR = Executors
            .newFixedThreadPool(RpcTestService.class.getSimpleName() + "_ASYNC", 1)
            .setDynamicThreadName(false);
    private final int rpcClientThreads;
    private final OutputStream log;
    private final LoopInterruptedCheck loopCheck;
    private final AtomicInteger countHolder = new AtomicInteger();
    private Instant writesStart;
    private final ILatencyReport latencyReportRequestReceived;

    public RpcTestService(final int rpcClientThreads) {
        this(rpcClientThreads, new Log(RpcTestService.class));
    }

    public RpcTestService(final int rpcClientThreads, final Log log) {
        this(rpcClientThreads, Slf4jStream.of(log).asInfo());
    }

    public RpcTestService(final int rpcClientThreads, final OutputStream log) {
        this.rpcClientThreads = rpcClientThreads;
        this.loopCheck = AChannelTest.newLoopInterruptedCheck(ALatencyChannelTest.FLUSH_INTERVAL * rpcClientThreads);
        this.log = log;
        this.latencyReportRequestReceived = AChannelTest.LATENCY_REPORT_FACTORY
                .newLatencyReport("rpc/1_" + RpcTestService.class.getSimpleName() + "_requestReceived");
    }

    private FDate handleRequest(final FDate request) throws IOException {
        if (writesStart == null) {
            synchronized (this) {
                if (writesStart == null) {
                    //don't count in connection establishment
                    writesStart = new Instant();
                }
            }
        }
        if (ALatencyChannelTest.DEBUG) {
            log.write("server request in\n".getBytes());
        }
        final FDate response = latencyReportRequestReceived.newResponseMessage(request).asFDate();
        //        FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        if (ALatencyChannelTest.DEBUG) {
            log.write(("server response out [" + response + "]\n").getBytes());
        }
        final int count = countHolder.incrementAndGet();
        if (loopCheck.checkNoInterrupt()) {
            ALatencyChannelTest.printProgress(log, "Writes", writesStart, count,
                    ALatencyChannelTest.MESSAGE_COUNT * rpcClientThreads);
        }
        return response;
    }

    @Override
    public FDate requestDefault(final FDate date) throws IOException {
        latencyReportRequestReceived.measureLatency(date);
        return handleRequest(date);
    }

    @Override
    public FDate requestTrueTrue(final FDate date) throws IOException {
        return requestDefault(date);
    }

    @Override
    public FDate requestFalseTrue(final FDate date) throws IOException {
        return requestDefault(date);
    }

    @Override
    public FDate requestTrueFalse(final FDate date) throws IOException {
        return requestDefault(date);
    }

    @Override
    public FDate requestFalseFalse(final FDate date) throws IOException {
        return requestDefault(date);
    }

    @Override
    public Future<FDate> requestFutureDefault(final FDate date) throws IOException {
        return ImmutableFuture.of(requestDefault(date));
    }

    @Override
    public Future<FDate> requestFutureTrueTrue(final FDate date) throws IOException {
        return requestFutureDefault(date);
    }

    @Override
    public Future<FDate> requestFutureFalseTrue(final FDate date) throws IOException {
        return requestFutureDefault(date);
    }

    @Override
    public Future<FDate> requestFutureTrueFalse(final FDate date) throws IOException {
        return requestFutureDefault(date);
    }

    @Override
    public Future<FDate> requestFutureFalseFalse(final FDate date) throws IOException {
        return requestFutureDefault(date);
    }

    @Override
    public Future<FDate> requestAsyncDefault(final FDate date) throws IOException {
        latencyReportRequestReceived.measureLatency(date);
        return ASYNC_EXECUTOR.submit(() -> handleRequest(date));
    }

    @Override
    public Future<FDate> requestAsyncTrueTrue(final FDate date) throws IOException {
        return requestAsyncDefault(date);
    }

    @Override
    public Future<FDate> requestAsyncFalseTrue(final FDate date) throws IOException {
        return requestAsyncDefault(date);
    }

    @Override
    public Future<FDate> requestAsyncTrueFalse(final FDate date) throws IOException {
        return requestAsyncDefault(date);
    }

    @Override
    public Future<FDate> requestAsyncFalseFalse(final FDate date) throws IOException {
        return requestAsyncDefault(date);
    }

    @Override
    public void close() throws IOException {
        latencyReportRequestReceived.close();
    }

}
