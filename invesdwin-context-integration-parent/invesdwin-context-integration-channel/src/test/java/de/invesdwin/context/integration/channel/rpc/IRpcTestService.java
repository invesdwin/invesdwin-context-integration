package de.invesdwin.context.integration.channel.rpc;

import java.io.IOException;

import de.invesdwin.util.time.date.FDate;

public interface IRpcTestService {

    FDate request(FDate date) throws IOException;

}
