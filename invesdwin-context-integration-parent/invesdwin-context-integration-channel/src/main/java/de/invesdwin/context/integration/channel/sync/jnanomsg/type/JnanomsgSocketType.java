package de.invesdwin.context.integration.channel.sync.jnanomsg.type;

import javax.annotation.concurrent.Immutable;

import nanomsg.AbstractSocket;
import nanomsg.pair.PairSocket;
import nanomsg.pipeline.PullSocket;
import nanomsg.pipeline.PushSocket;
import nanomsg.pubsub.PubSocket;
import nanomsg.pubsub.SubSocket;
import nanomsg.reqrep.RepSocket;
import nanomsg.reqrep.ReqSocket;

@Immutable
public enum JnanomsgSocketType implements IJnanomsgSocketType {
    PAIR {
        @Override
        public AbstractSocket newWriterSocket() {
            return new PairSocket();
        }

        @Override
        public AbstractSocket newReaderSocket() {
            return new PairSocket();
        }

        @Override
        public boolean isPublishSubscribeTopic() {
            return false;
        }
    },
    PUBSUB {
        @Override
        public AbstractSocket newWriterSocket() {
            return new PubSocket();
        }

        @Override
        public AbstractSocket newReaderSocket() {
            return new SubSocket();
        }

        @Override
        public boolean isPublishSubscribeTopic() {
            return true;
        }
    },
    PUSHPULL {
        @Override
        public AbstractSocket newWriterSocket() {
            return new PushSocket();
        }

        @Override
        public AbstractSocket newReaderSocket() {
            return new PullSocket();
        }

        @Override
        public boolean isPublishSubscribeTopic() {
            return false;
        }
    },
    REQREP {
        @Override
        public AbstractSocket newWriterSocket() {
            return new ReqSocket();
        }

        @Override
        public AbstractSocket newReaderSocket() {
            return new RepSocket();
        }

        @Override
        public boolean isPublishSubscribeTopic() {
            return false;
        }
    },;

}
