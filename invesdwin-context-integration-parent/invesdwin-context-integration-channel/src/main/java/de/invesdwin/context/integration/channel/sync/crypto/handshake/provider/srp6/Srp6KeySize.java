package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum Srp6KeySize {
    _256(256),
    _512(512),
    _768(768),
    _1024(1024),
    _1536(1536),
    _2048(2048),
    _3072(3072),
    _4096(4096),
    _6144(6144),
    _8192(8192);

    /**
     * NOTE: Please do only use key length >= 2048 bit in production. You can do so by using Srp6_2048 or Srp6_4096.
     * 
     * (https://docs.rs/srp6/1.0.0-alpha.5/srp6/)
     * 
     * => we go with 4096, the same as RSA
     */
    public static final Srp6KeySize DEFAULT = Srp6KeySize._4096;

    private int bits;

    Srp6KeySize(final int bits) {
        this.bits = bits;
    }

    public int getBits() {
        return bits;
    }

}
