package de.invesdwin.context.integration.channel.zeromq.czmq.type;

import javax.annotation.concurrent.Immutable;

import org.zeromq.czmq.Zsock;

import de.invesdwin.context.integration.channel.zeromq.ZeromqSocketTypes;

@Immutable
public enum CzmqSocketType implements ICzmqSocketType {
    PAIR {
        @Override
        public ICzmqSocketFactory newWriterSocketFactory() {
            return new ICzmqSocketFactory() {

                @Override
                public Zsock newSocket(final String endpoint, final String topic) {
                    return Zsock.newPair(endpoint);
                }

                @Override
                public int getSocketType() {
                    return ZeromqSocketTypes.PAIR;
                }
            };
        }

        @Override
        public ICzmqSocketFactory newReaderSocketFactory() {
            return newWriterSocketFactory();
        }
    },
    PUSHPULL {
        @Override
        public ICzmqSocketFactory newWriterSocketFactory() {
            return new ICzmqSocketFactory() {

                @Override
                public Zsock newSocket(final String endpoint, final String topic) {
                    return Zsock.newPush(endpoint);
                }

                @Override
                public int getSocketType() {
                    return ZeromqSocketTypes.PUSH;
                }
            };
        }

        @Override
        public ICzmqSocketFactory newReaderSocketFactory() {
            return new ICzmqSocketFactory() {

                @Override
                public Zsock newSocket(final String endpoint, final String topic) {
                    return Zsock.newPull(endpoint);
                }

                @Override
                public int getSocketType() {
                    return ZeromqSocketTypes.PULL;
                }
            };
        }
    },
    PUBSUB {
        @Override
        public ICzmqSocketFactory newWriterSocketFactory() {
            return new ICzmqSocketFactory() {

                @Override
                public Zsock newSocket(final String endpoint, final String topic) {
                    return Zsock.newPub(endpoint);
                }

                @Override
                public int getSocketType() {
                    return ZeromqSocketTypes.PUB;
                }
            };
        }

        @Override
        public ICzmqSocketFactory newReaderSocketFactory() {
            return new ICzmqSocketFactory() {

                @Override
                public Zsock newSocket(final String endpoint, final String topic) {
                    return Zsock.newSub(endpoint, topic);
                }

                @Override
                public int getSocketType() {
                    return ZeromqSocketTypes.SUB;
                }
            };
        }
    };

    CzmqSocketType() {
    }

}
