package de.invesdwin.context.integration.channel.sync.aeron;

import javax.annotation.concurrent.Immutable;

// https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/c/README.md
@Immutable
public enum AeronMediaDriverMode {
    EMBEDDED,
    NATIVE;
}
