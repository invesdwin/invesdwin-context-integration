package de.invesdwin.context.integration.channel.rpc;

import java.io.IOException;

import de.invesdwin.util.time.date.FDate;

public interface IRpcTestService {

    //    @Fast
    //TODO: test with future as soon as client supports it
    FDate request(FDate date) throws IOException;

}
