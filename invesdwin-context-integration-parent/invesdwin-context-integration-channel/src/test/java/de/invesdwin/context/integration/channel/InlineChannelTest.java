package de.invesdwin.context.integration.channel;

import javax.annotation.concurrent.Immutable;

@Immutable
public class InlineChannelTest extends AChannelTest {

    @Override
    protected void init() {
        //noop, otherwise ignite fails invesdwin initialization
    }

}
