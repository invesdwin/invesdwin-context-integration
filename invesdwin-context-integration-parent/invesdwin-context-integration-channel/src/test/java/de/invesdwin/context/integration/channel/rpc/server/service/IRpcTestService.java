package de.invesdwin.context.integration.channel.rpc.server.service;

import java.io.IOException;
import java.util.concurrent.Future;

import de.invesdwin.util.time.date.FDate;

public interface IRpcTestService {

    FDate requestDefault(FDate date) throws IOException;

    @Blocking(client = true, server = true)
    FDate requestTrueTrue(FDate date) throws IOException;

    @Blocking(client = false, server = true)
    FDate requestFalseTrue(FDate date) throws IOException;

    @Blocking(client = true, server = false)
    FDate requestTrueFalse(FDate date) throws IOException;

    @Blocking(client = false, server = false)
    FDate requestFalseFalse(FDate date) throws IOException;

    Future<FDate> requestFutureDefault(FDate date) throws IOException;

    @Blocking(client = true, server = true)
    Future<FDate> requestFutureTrueTrue(FDate date) throws IOException;

    @Blocking(client = false, server = true)
    Future<FDate> requestFutureFalseTrue(FDate date) throws IOException;

    @Blocking(client = true, server = false)
    Future<FDate> requestFutureTrueFalse(FDate date) throws IOException;

    @Blocking(client = false, server = false)
    Future<FDate> requestFutureFalseFalse(FDate date) throws IOException;

}
