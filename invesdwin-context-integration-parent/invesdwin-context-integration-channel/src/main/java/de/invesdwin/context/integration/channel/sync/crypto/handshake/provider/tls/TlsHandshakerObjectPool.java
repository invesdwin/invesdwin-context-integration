package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class TlsHandshakerObjectPool extends ATimeoutObjectPool<TlsHandshaker> {

    public static final TlsHandshakerObjectPool INSTANCE = new TlsHandshakerObjectPool();

    private TlsHandshakerObjectPool() {
        super(Duration.ONE_MINUTE, new Duration(10, FTimeUnit.SECONDS));
    }

    @Override
    protected TlsHandshaker newObject() {
        return new TlsHandshaker();
    }

    @Override
    public void invalidateObject(final TlsHandshaker element) {
        element.reset();
    }

    @Override
    protected boolean passivateObject(final TlsHandshaker element) {
        element.reset();
        return true;
    }

}
