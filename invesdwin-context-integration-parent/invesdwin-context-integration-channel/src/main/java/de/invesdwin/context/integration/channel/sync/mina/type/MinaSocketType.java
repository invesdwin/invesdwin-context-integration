package de.invesdwin.context.integration.channel.sync.mina.type;

import java.util.concurrent.Executor;

import javax.annotation.concurrent.Immutable;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.transport.socket.apr.AprDatagramAcceptor2;
import org.apache.mina.transport.socket.apr.AprDatagramConnector2;
import org.apache.mina.transport.socket.apr.AprDatagramIoProcessor;
import org.apache.mina.transport.socket.apr.AprSctpAcceptor;
import org.apache.mina.transport.socket.apr.AprSctpConnector;
import org.apache.mina.transport.socket.apr.AprSocketAcceptor;
import org.apache.mina.transport.socket.apr.AprSocketConnector;
import org.apache.mina.transport.socket.apr.FixedAprIoProcessor;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.apache.mina.transport.vmpipe.VmPipeAcceptor;
import org.apache.mina.transport.vmpipe.VmPipeConnector;

@Immutable
public enum MinaSocketType implements IMinaSocketType {
    VmPipe {
        @Override
        public IoAcceptor newAcceptor() {
            return new VmPipeAcceptor();
        }

        @Override
        public IoConnector newConnector() {
            return new VmPipeConnector();
        }

        @Override
        public boolean isUnbindAcceptor() {
            return false;
        }

        @Override
        public boolean isValidateConnect() {
            return false;
        }

        @Override
        public boolean isNative() {
            return false;
        }
    },
    NioTcp {
        @Override
        public IoAcceptor newAcceptor() {
            return new NioSocketAcceptor();
        }

        @Override
        public IoConnector newConnector() {
            return new NioSocketConnector();
        }

        @Override
        public boolean isUnbindAcceptor() {
            return true;
        }

        @Override
        public boolean isValidateConnect() {
            return false;
        }

        @Override
        public boolean isNative() {
            return false;
        }
    },
    NioUdp {
        @Override
        public IoAcceptor newAcceptor() {
            return new NioDatagramAcceptor();
        }

        @Override
        public IoConnector newConnector() {
            return new NioDatagramConnector();
        }

        @Override
        public boolean isUnbindAcceptor() {
            return false;
        }

        @Override
        public boolean isValidateConnect() {
            return false;
        }

        @Override
        public boolean isNative() {
            return false;
        }

    },
    /**
     * No AprUdp right now: https://issues.apache.org/jira/browse/DIRMINA-484
     * https://issues.apache.org/jira/browse/DIRMINA-438
     */
    AprTcp {
        @Override
        public IoAcceptor newAcceptor() {
            return new AprSocketAcceptor(new FixedAprIoProcessor(newDefaultExecutor()));
        }

        @Override
        public IoConnector newConnector() {
            return new AprSocketConnector(new FixedAprIoProcessor(newDefaultExecutor()));
        }

        @Override
        public boolean isUnbindAcceptor() {
            return true;
        }

        @Override
        public boolean isValidateConnect() {
            return true;
        }

        @Override
        public boolean isNative() {
            return true;
        }
    },
    AprUdp {
        @Override
        public IoAcceptor newAcceptor() {
            return new AprDatagramAcceptor2();
        }

        @Override
        public IoConnector newConnector() {
            return new AprDatagramConnector2(new AprDatagramIoProcessor(newDefaultExecutor()));
        }

        @Override
        public boolean isUnbindAcceptor() {
            return false;
        }

        @Override
        public boolean isValidateConnect() {
            return false;
        }

        @Override
        public boolean isNative() {
            return true;
        }
    },
    AprSctp {
        @Override
        public IoAcceptor newAcceptor() {
            return new AprSctpAcceptor(new FixedAprIoProcessor(newDefaultExecutor()));
        }

        @Override
        public IoConnector newConnector() {
            return new AprSctpConnector(new FixedAprIoProcessor(newDefaultExecutor()));
        }

        @Override
        public boolean isUnbindAcceptor() {
            return true;
        }

        @Override
        public boolean isValidateConnect() {
            return true;
        }

        @Override
        public boolean isNative() {
            return true;
        }
    };

    private static Executor newDefaultExecutor() {
        final Executor executor = java.util.concurrent.Executors.newCachedThreadPool();
        // Set a default reject handler
        ((java.util.concurrent.ThreadPoolExecutor) executor)
                .setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }
}
