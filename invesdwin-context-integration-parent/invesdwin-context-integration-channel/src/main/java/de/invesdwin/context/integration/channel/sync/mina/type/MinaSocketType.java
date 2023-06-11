package de.invesdwin.context.integration.channel.sync.mina.type;

import java.util.concurrent.Executor;

import javax.annotation.concurrent.Immutable;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.SimpleIoProcessorPool;
import org.apache.mina.transport.socket.apr.AprSctpAcceptor;
import org.apache.mina.transport.socket.apr.AprSctpConnector;
import org.apache.mina.transport.socket.apr.AprSession;
import org.apache.mina.transport.socket.apr.AprSocketAcceptor;
import org.apache.mina.transport.socket.apr.AprSocketConnector;
import org.apache.mina.transport.socket.apr.FixedAprIoProcessor;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.mina.transport.socket.nio.NioProcessor;
import org.apache.mina.transport.socket.nio.NioSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.apache.mina.transport.vmpipe.VmPipeAcceptor;
import org.apache.mina.transport.vmpipe.VmPipeConnector;

@Immutable
public enum MinaSocketType implements IMinaSocketType {
    VmPipe {
        @Override
        public IoAcceptor newAcceptor(final Executor executor, final int processorCount) {
            return new VmPipeAcceptor(executor);
        }

        @Override
        public IoConnector newConnector(final Executor executor, final int processorCount) {
            return new VmPipeConnector(executor);
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
        public IoAcceptor newAcceptor(final Executor executor, final int processorCount) {
            return new NioSocketAcceptor(executor,
                    new SimpleIoProcessorPool<NioSession>(NioProcessor.class, processorCount));
        }

        @Override
        public IoConnector newConnector(final Executor executor, final int processorCount) {
            return new NioSocketConnector(executor,
                    new SimpleIoProcessorPool<NioSession>(NioProcessor.class, processorCount));
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
        public IoAcceptor newAcceptor(final Executor executor, final int processorCount) {
            return new NioDatagramAcceptor(executor);
        }

        @Override
        public IoConnector newConnector(final Executor executor, final int processorCount) {
            return new NioDatagramConnector(new SimpleIoProcessorPool<NioSession>(NioProcessor.class, processorCount));
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
        public IoAcceptor newAcceptor(final Executor executor, final int processorCount) {
            return new AprSocketAcceptor(executor,
                    new SimpleIoProcessorPool<AprSession>(FixedAprIoProcessor.class, processorCount));
        }

        @Override
        public IoConnector newConnector(final Executor executor, final int processorCount) {
            return new AprSocketConnector(executor,
                    new SimpleIoProcessorPool<AprSession>(FixedAprIoProcessor.class, processorCount));
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
    AprSctp {
        @Override
        public IoAcceptor newAcceptor(final Executor executor, final int processorCount) {
            return new AprSctpAcceptor(executor,
                    new SimpleIoProcessorPool<AprSession>(FixedAprIoProcessor.class, processorCount));
        }

        @Override
        public IoConnector newConnector(final Executor executor, final int processorCount) {
            return new AprSctpConnector(executor,
                    new SimpleIoProcessorPool<AprSession>(FixedAprIoProcessor.class, processorCount));
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

}
