package de.invesdwin.context.integration.channel.sync.mina.type;

import javax.annotation.concurrent.Immutable;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.transport.socket.apr.AprSocketAcceptor;
import org.apache.mina.transport.socket.apr.AprSocketConnector;
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

    },
    /**
     * No AprUdp right now: https://issues.apache.org/jira/browse/DIRMINA-484
     * https://issues.apache.org/jira/browse/DIRMINA-438
     */
    AprTcp {
        @Override
        public IoAcceptor newAcceptor() {
            return new AprSocketAcceptor();
        }

        @Override
        public IoConnector newConnector() {
            return new AprSocketConnector();
        }
    },
}
