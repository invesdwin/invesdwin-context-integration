package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.bean.AValueObject;

@Immutable
public class Srp6ServerStep1LookupInput extends AValueObject {

    private final String userIdHash;

    public Srp6ServerStep1LookupInput(final String userIdHash) {
        this.userIdHash = userIdHash;
    }

    public String getUserIdHash() {
        return userIdHash;
    }

}
