package de.invesdwin.context.integration.channel.rpc.base.server.service;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.time.date.FDate;

@Immutable
public enum RpcTestServiceMode {
    requestDefault {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return service.requestDefault(date);
        }
    },
    requestTrueTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return service.requestTrueTrue(date);
        }
    },
    requestFalseTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return service.requestFalseTrue(date);
        }
    },
    requestTrueFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return service.requestTrueFalse(date);
        }
    },
    requestFalseFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return service.requestFalseFalse(date);
        }
    },
    requestFutureDefault {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestFutureDefault(date));
        }
    },
    requestFutureTrueTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestFutureTrueTrue(date));
        }
    },
    requestFutureFalseTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestFutureFalseTrue(date));
        }
    },
    requestFutureTrueFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestFutureTrueFalse(date));
        }
    },
    requestFutureFalseFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestFutureFalseFalse(date));
        }
    },
    requestAsyncDefault {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestAsyncDefault(date));
        }
    },
    requestAsyncTrueTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestAsyncTrueTrue(date));
        }
    },
    requestAsyncFalseTrue {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestAsyncFalseTrue(date));
        }
    },
    requestAsyncTrueFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestAsyncTrueFalse(date));
        }
    },
    requestAsyncFalseFalse {
        @Override
        public FDate request(final IRpcTestService service, final FDate date) throws IOException {
            return Futures.getNoInterrupt(service.requestAsyncFalseFalse(date));
        }
    };

    public abstract FDate request(IRpcTestService service, FDate date) throws IOException;
}
