package org.apache.mina.transport.socket.apr;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class AprLibraryAccessor {

    private AprLibraryAccessor() {}

    public static long getRootPool() {
        return AprLibrary.getInstance().getRootPool();
    }

}
