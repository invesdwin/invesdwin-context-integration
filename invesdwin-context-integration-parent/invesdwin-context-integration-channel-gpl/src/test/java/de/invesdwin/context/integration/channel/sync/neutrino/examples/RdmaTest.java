// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.neutrino.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hhu.bsinfo.neutrino.struct.field.NativeLinkedList;
import de.hhu.bsinfo.neutrino.verbs.CompletionQueue;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest.Builder;
import de.hhu.bsinfo.neutrino.verbs.WorkCompletion;

// @CommandLine.Command(name = "rdma-test", description = "Starts a simple InfiniBand RDMA test, using the neutrino
// core.%n", showDefaultValues = true, separator = " ")
@NotThreadSafe
public class RdmaTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RdmaTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULT_QUEUE_SIZE = 100;
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_OPERATION_COUNT = 1048576;

    private static final String SOCKET_CLOSE_SIGNAL = "close";

    private enum Mode {
        READ,
        WRITE
    }

    //    @CommandLine.Option(names = "--server", description = "Runs this instance in server mode.")
    private boolean isServer;

    //    @CommandLine.Option(names = { "-p", "--port" }, description = "The port the server will listen on.")
    private final int port = DEFAULT_SERVER_PORT;

    //    @CommandLine.Option(names = { "-d", "--device" }, description = "Sets the InfiniBand device to be used.")
    private final int device = 0;

    //    @CommandLine.Option(names = { "-s", "--size" }, description = "Sets the buffer size.")
    private final int bufferSize = DEFAULT_BUFFER_SIZE;

    //    @CommandLine.Option(names = { "-c", "--connect" }, description = "The server to connect to.")
    private InetSocketAddress serverAddress;

    //    @CommandLine.Option(names = { "-q",
    //            "--queue-size" }, description = "The queue size to be used for the queue pair and completion queue.")
    private final int queueSize = DEFAULT_QUEUE_SIZE;

    //    @CommandLine.Option(names = { "-n", "--count" }, description = "The amount of RDMA operations to be performed.")
    private final int operationCount = DEFAULT_OPERATION_COUNT;

    //    @CommandLine.Option(names = { "-m", "--mode" }, description = "Set the rdma operation mode (read/write).")
    private final Mode mode = Mode.WRITE;

    private ScatterGatherElement scatterGatherElement;
    private SendWorkRequest[] sendWorkRequests;

    private final NativeLinkedList<SendWorkRequest> sendList = new NativeLinkedList<>();

    private CompletionQueue.WorkCompletionArray completionArray;

    private RdmaContext context;
    private Result result;

    public static void main(final String[] args) {
        final InetSocketAddress address = new InetSocketAddress("192.168.0.20", DEFAULT_SERVER_PORT);

        new Thread() {
            @Override
            public void run() {
                final RdmaTest server = new RdmaTest();
                server.isServer = true;
                server.serverAddress = address;
                try {
                    server.call();
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();

        final RdmaTest client = new RdmaTest();
        client.isServer = false;
        client.serverAddress = address;
        try {
            client.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void call() throws Exception {
        if (!isServer && serverAddress == null) {
            LOGGER.error("Please specify the server address");
            return null;
        }

        completionArray = new CompletionQueue.WorkCompletionArray(queueSize);

        context = new RdmaContext(device, queueSize, bufferSize);

        scatterGatherElement = new ScatterGatherElement(context.getLocalBuffer().getHandle(),
                context.getLocalBuffer().getNativeSize(), context.getLocalBuffer().getLocalKey());

        sendWorkRequests = new SendWorkRequest[queueSize];

        if (isServer) {
            startServer();
        } else {
            startClient();
        }

        context.close();

        if (isServer) {
            LOGGER.info(result.toString());
        }

        return null;
    }

    private void startServer() throws IOException {
        final ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getByName("192.168.0.20"));
        final Socket socket = serverSocket.accept();

        context.connect(socket);

        final Builder sendBuilder = new SendWorkRequest.RdmaBuilder(
                mode == Mode.WRITE ? SendWorkRequest.OpCode.RDMA_WRITE : SendWorkRequest.OpCode.RDMA_READ,
                scatterGatherElement, context.getRemoteBufferInfo().getAddress(),
                context.getRemoteBufferInfo().getRemoteKey()).withSendFlags(SendWorkRequest.SendFlag.SIGNALED);

        for (int i = 0; i < queueSize; i++) {
            sendWorkRequests[i] = sendBuilder.build();
        }

        for (int i = 0; i < queueSize; i++) {
            sendWorkRequests[i] = sendBuilder.build();
        }

        LOGGER.info("Starting to " + mode.toString().toLowerCase() + " via RDMA");

        int operationsLeft = operationCount;
        int pendingCompletions = 0;

        final long startTime = System.nanoTime();

        while (operationsLeft > 0) {
            int batchSize = queueSize - pendingCompletions;

            if (batchSize > operationsLeft) {
                batchSize = operationsLeft;
            }

            performOperations(batchSize);

            pendingCompletions += batchSize;
            operationsLeft -= batchSize;

            pendingCompletions -= poll();
        }

        while (pendingCompletions > 0) {
            pendingCompletions -= poll();
        }

        final long time = System.nanoTime() - startTime;
        result = new Result(operationCount, bufferSize, time);

        LOGGER.info("Finished " + mode.toString().toLowerCase() + "ing via RDMA");

        socket.getOutputStream().write(SOCKET_CLOSE_SIGNAL.getBytes());

        LOGGER.info("Sent '" + SOCKET_CLOSE_SIGNAL + "' to client");

        socket.close();
    }

    private void startClient() throws IOException {
        final Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort());

        context.connect(socket);

        LOGGER.info("Waiting for server to finish RDMA operations");

        final String serverString = new String(socket.getInputStream().readAllBytes());

        if (serverString.equals(SOCKET_CLOSE_SIGNAL)) {
            LOGGER.info("Received '" + serverString + "' from server");
        } else if (serverString.isEmpty()) {
            LOGGER.error("Lost connection to server");
        } else {
            LOGGER.error("Received invalid string '" + serverString + "' from server");
        }

        socket.close();
    }

    private void performOperations(final int amount) throws IOException {
        if (amount == 0) {
            return;
        }

        sendList.clear();

        for (int i = 0; i < amount; i++) {
            sendList.add(sendWorkRequests[i]);
        }

        context.getQueuePair().postSend(sendList);
    }

    private int poll() throws IOException {
        final CompletionQueue completionQueue = context.getCompletionQueue();

        completionQueue.poll(completionArray);

        for (int i = 0; i < completionArray.getLength(); i++) {
            final WorkCompletion completion = completionArray.get(i);

            if (completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                LOGGER.error("Work completion failed with error [{}]: {}", completion.getStatus(),
                        completion.getStatusMessage());
                System.exit(1);
            }
        }

        return completionArray.getLength();
    }
}
