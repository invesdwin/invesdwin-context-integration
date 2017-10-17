package de.invesdwin.context.integration.ws.registry;

public interface IRestRegistryService {

    String REGISTRY = "registry";

    String SERVICE_NAME = "serviceName";
    String SERVICE_NAME_PARAM = "{" + SERVICE_NAME + "}";
    String ACCESS_URI = "accessUri";
    String ACCESS_URI_PARAM = "{" + ACCESS_URI + "}";

    String REGISTER_SERVICE_BINDING = "registerservicebinding_" + SERVICE_NAME_PARAM + "_" + ACCESS_URI_PARAM;
    String UNREGISTER_SERVICE_BINDING = "unregisterservicebinding_" + SERVICE_NAME_PARAM + "_" + ACCESS_URI_PARAM;
    String QUERY_SERVICE_BINDINGS = "queryservicebindings_" + SERVICE_NAME_PARAM;
    String INFO = "info";
    String AVAILABLE = "available";
    String CLIENTIP = "clientip";

}
