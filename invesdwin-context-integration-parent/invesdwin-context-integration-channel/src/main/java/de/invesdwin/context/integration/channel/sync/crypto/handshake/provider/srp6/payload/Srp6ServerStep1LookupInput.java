package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Srp6ServerStep1LookupInput {

    private final String userId;

    public Srp6ServerStep1LookupInput(final String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

}
