package de.invesdwin.common.integration.ws.registry;

import java.net.URI;
import java.util.List;

import org.springframework.ws.client.support.destination.DestinationProvider;

public interface IDestinationProvider extends DestinationProvider {

    List<URI> getDestinations();

    void reset();

    String getServiceName();

}
