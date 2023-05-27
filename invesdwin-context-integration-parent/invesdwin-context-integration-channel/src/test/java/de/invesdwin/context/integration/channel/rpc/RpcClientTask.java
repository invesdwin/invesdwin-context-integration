package de.invesdwin.context.integration.channel.rpc;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.AChannelTest.ClientTask;
import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class RpcClientTask implements Runnable {

    private final OutputStream log;
    private final SynchronousEndpointClient<IRpcTestService> client;
    private final String clientId;

    public RpcClientTask(final SynchronousEndpointClient<IRpcTestService> client, final String clientId) {
        this(new Log(ClientTask.class), client, clientId);
    }

    public RpcClientTask(final Log log, final SynchronousEndpointClient<IRpcTestService> client,
            final String clientId) {
        this(Slf4jStream.of(log).asInfo(), client, clientId);
    }

    public RpcClientTask(final OutputStream log, final SynchronousEndpointClient<IRpcTestService> client,
            final String clientId) {
        this.log = log;
        this.client = client;
        this.clientId = clientId;
    }

    @Override
    public void run() {
        Instant readsStart = new Instant();
        FDate prevValue = null;
        int count = 0;
        try {
            readsStart = new Instant();
            try (ICloseableIterator<FDate> values = AChannelTest.newValues().iterator()) {
                while (count < AChannelTest.VALUES) {
                    if (AChannelTest.DEBUG) {
                        log.write((clientId + ": client request out\n").getBytes());
                    }
                    final FDate request = values.next();
                    final FDate response = client.getService().request(request);
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