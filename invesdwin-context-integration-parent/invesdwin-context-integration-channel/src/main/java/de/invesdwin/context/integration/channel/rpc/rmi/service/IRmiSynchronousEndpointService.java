package de.invesdwin.context.integration.channel.rpc.rmi.service;

import java.rmi.RemoteException;

public interface IRmiSynchronousEndpointService {

    byte[] call(byte[] request) throws RemoteException;

}
