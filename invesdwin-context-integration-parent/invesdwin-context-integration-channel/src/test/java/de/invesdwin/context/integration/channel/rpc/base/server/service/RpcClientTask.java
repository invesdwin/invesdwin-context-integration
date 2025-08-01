package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.report.ILatencyReport;
import de.invesdwin.context.integration.channel.report.ILatencyReportFactory;
import de.invesdwin.context.integration.channel.rpc.base.client.IRpcSynchronousEndpointClient;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.IFDateProvider;

@NotThreadSafe
public class RpcClientTask implements Runnable {

    private final AChannelTest parent;
    private final OutputStream log;
    private final IRpcSynchronousEndpointClient<IRpcTestService> client;
    private final String clientId;
    private final RpcTestServiceMode mode;

    public RpcClientTask(final AChannelTest parent, final IRpcSynchronousEndpointClient<IRpcTestService> client,
            final String clientId, final RpcTestServiceMode mode) {
        this(parent, new Log(LatencyClientTask.class), client, clientId, mode);
    }

    public RpcClientTask(final AChannelTest parent, final Log log,
            final IRpcSynchronousEndpointClient<IRpcTestService> client, final String clientId,
            final RpcTestServiceMode mode) {
        this(parent, Slf4jStream.of(log).asInfo(), client, clientId, mode);
    }

    public RpcClientTask(final AChannelTest parent, final OutputStream log,
            final IRpcSynchronousEndpointClient<IRpcTestService> client, final String clientId,
            final RpcTestServiceMode mode) {
        this.parent = parent;
        this.log = log;
        this.client = client;
        this.clientId = clientId;
        this.mode = mode;
    }

    @Override
    public void run() {
        int count = -parent.getWarmupMessageCount();
        Instant readsStart = new Instant();
        FDate prevValue = null;
        final ILatencyReportFactory latencyReportFactory = AChannelTest.LATENCY_REPORT_FACTORY;
        final ILatencyReport latencyReportResponseReceived = latencyReportFactory
                .newLatencyReport("rpc/2_" + RpcClientTask.class.getSimpleName() + "_responseReceived");
        final ILatencyReport latencyReportRequestResponseRoundtrip = latencyReportFactory
                .newLatencyReport("rpc/3_" + RpcClientTask.class.getSimpleName() + "_requestResponseRoundtrip");
        try {
            try (ICloseableIterator<? extends IFDateProvider> values = latencyReportRequestResponseRoundtrip
                    .newRequestMessages()
                    .iterator()) {
                while (count < parent.getMessageCount()) {
                    if (count == 0) {
                        //don't count in connection establishment
                        readsStart = new Instant();
                    }
                    if (AChannelTest.DEBUG) {
                        log.write((clientId + ": client request out\n").getBytes());
                    }
                    final IFDateProvider requestProvider = values.next();
                    final FDate request = requestProvider.asFDate();
                    final FDate response = mode.request(client.getService(), request);
                    final FDate arrivalTimestamp = latencyReportRequestResponseRoundtrip.newArrivalTimestamp()
                            .asFDate();
                    latencyReportResponseReceived.measureLatency(count, response, arrivalTimestamp);
                    latencyReportRequestResponseRoundtrip.measureLatency(count, request, arrivalTimestamp);
                    if (AChannelTest.DEBUG) {
                        log.write((clientId + ": client response in [" + response + "]\n").getBytes());
                    }
                    Assertions.checkNotNull(response);
                    latencyReportRequestResponseRoundtrip.validateResponse(request, response);
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(response));
                    }
                    prevValue = response;
                    count++;
                }
            }
        } catch (final EOFException e) {
            //writer closed
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.checkEquals(parent.getMessageCount(), count);
        try {
            AChannelTest.printProgress(log, clientId + ": ReadsFinished", readsStart, count, parent.getMessageCount());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        latencyReportResponseReceived.close();
        latencyReportRequestResponseRoundtrip.close();
    }

}