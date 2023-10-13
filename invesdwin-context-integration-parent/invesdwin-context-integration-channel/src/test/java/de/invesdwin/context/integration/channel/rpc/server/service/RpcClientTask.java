package de.invesdwin.context.integration.channel.rpc.server.service;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.AChannelTest.ClientTask;
import de.invesdwin.context.integration.channel.rpc.client.ISynchronousEndpointClient;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class RpcClientTask implements Runnable {

    private final OutputStream log;
    private final ISynchronousEndpointClient<IRpcTestService> client;
    private final String clientId;
    private final RpcTestServiceMode mode;

    public RpcClientTask(final ISynchronousEndpointClient<IRpcTestService> client, final String clientId,
            final RpcTestServiceMode mode) {
        this(new Log(ClientTask.class), client, clientId, mode);
    }

    public RpcClientTask(final Log log, final ISynchronousEndpointClient<IRpcTestService> client, final String clientId,
            final RpcTestServiceMode mode) {
        this(Slf4jStream.of(log).asInfo(), client, clientId, mode);
    }

    public RpcClientTask(final OutputStream log, final ISynchronousEndpointClient<IRpcTestService> client,
            final String clientId, final RpcTestServiceMode mode) {
        this.log = log;
        this.client = client;
        this.clientId = clientId;
        this.mode = mode;
    }

    @Override
    public void run() {
        Instant readsStart = new Instant();
        FDate prevValue = null;
        int count = 0;
        try {
            try (ICloseableIterator<FDate> values = AChannelTest.newValues().iterator()) {
                while (count < AChannelTest.VALUES) {
                    if (AChannelTest.DEBUG) {
                        log.write((clientId + ": client request out\n").getBytes());
                    }
                    final FDate request = values.next();
                    final FDate response = mode.request(client.getService(), request);
                    if (count == 0) {
                        //don't count in connection establishment
                        readsStart = new Instant();
                    }
                    if (AChannelTest.DEBUG) {
                        log.write((clientId + ": client response in [" + response + "]\n").getBytes());
                    }
                    Assertions.checkNotNull(response);
                    Assertions.checkEquals(request.addMilliseconds(1), response);
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
        Assertions.checkEquals(AChannelTest.VALUES, count);
        try {
            AChannelTest.printProgress(log, clientId + ": ReadsFinished", readsStart, AChannelTest.VALUES,
                    AChannelTest.VALUES);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}