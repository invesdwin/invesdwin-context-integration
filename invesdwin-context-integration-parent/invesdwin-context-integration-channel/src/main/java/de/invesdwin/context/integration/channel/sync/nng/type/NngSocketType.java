package de.invesdwin.context.integration.channel.sync.nng.type;

import javax.annotation.concurrent.Immutable;

import io.sisu.nng.NngException;
import io.sisu.nng.Socket;
import io.sisu.nng.pair.Pair0Socket;
import io.sisu.nng.pair.Pair1Socket;
import io.sisu.nng.pipeline.Pull0Socket;
import io.sisu.nng.pipeline.Push0Socket;
import io.sisu.nng.pubsub.Pub0Socket;
import io.sisu.nng.pubsub.Sub0Socket;
import io.sisu.nng.reqrep.Rep0Socket;
import io.sisu.nng.reqrep.Req0Socket;

@Immutable
public enum NngSocketType implements INngSocketType {
    PAIR {
        @Override
        public Socket newWriterSocket() throws NngException {
            return new Pair0Socket();
        }

        @Override
        public Socket newReaderSocket() throws NngException {
            return new Pair0Socket();
        }
    },
    PAIR_1 {
        @Override
        public Socket newWriterSocket() throws NngException {
            return new Pair1Socket();
        }

        @Override
        public Socket newReaderSocket() throws NngException {
            return new Pair1Socket();
        }
    },
    PUBSUB {
        @Override
        public Socket newWriterSocket() throws NngException {
            return new Pub0Socket();
        }

        @Override
        public Socket newReaderSocket() throws NngException {
            return new Sub0Socket();
        }
    },
    PUSHPULL {
        @Override
        public Socket newWriterSocket() throws NngException {
            return new Push0Socket();
        }

        @Override
        public Socket newReaderSocket() throws NngException {
            return new Pull0Socket();
        }
    },
    REQREP {
        @Override
        public Socket newWriterSocket() throws NngException {
            return new Req0Socket();
        }

        @Override
        public Socket newReaderSocket() throws NngException {
            return new Rep0Socket();
        }
    },;

}
