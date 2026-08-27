package org.apache.mina.transport.socket.apr;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class AprSessionAccessor {

    private AprSessionAccessor() {}

    public static long getDescriptor(final AprSession session) {
        return session.getDescriptor();
    }

}
