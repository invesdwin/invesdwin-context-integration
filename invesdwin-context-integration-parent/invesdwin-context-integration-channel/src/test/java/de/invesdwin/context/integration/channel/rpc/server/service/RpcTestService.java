package de.invesdwin.context.integration.channel.rpc.server.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.Immutable;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.future.ImmutableFuture;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class RpcTestService implements IRpcTestService {

    private static final int FLUSH_INTERVAL = AChannelTest.FLUSH_INTERVAL * AChannelTest.RPC_CLIENT_COUNT;
    private final OutputStream log;
    private final AtomicInteger countHolder = new AtomicInteger();
    private Instant writesStart;

    public RpcTestService() {
        this(new Log(RpcTestService.class));
    }

    public RpcTestService(final Log log) {
        this(Slf4jStream.of(log).asInfo());
    }

    public RpcTestService(final OutputStream log) {
        this.log = log;
    }

    @Override
    public FDate requestDefault(final FDate date) throws IOException {
        if (writesStart == null) {
            synchronized (this) {
                if (writesStart == null) {
                    writesStart = new Instant();
                }
            }
        }
        if (AChannelTest.DEBUG) {
            log.write("server request in\n".getBytes());
        }
        final FDate response = date.addMilliseconds(1);
        //        FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        if (AChannelTest.DEBUG) {
            log.write(("server response out [" + response + "]\n").getBytes());
        }
        final int count = countHolder.incrementAndGet();
        if (count % FLUSH_INTERVAL == 0) {
            AChannelTest.printProgress(log, "Writes", writesStart, count,
                    AChannelTest.VALUES * AChannelTest.RPC_CLIENT_COUNT);
        }
        return response;
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

}
