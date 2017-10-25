package de.invesdwin.context.integration.ws.registry;

public interface IRestRegistryService {

    String REGISTRY = "registry";

    String SERVICE_NAME = "serviceName";
    String SERVICE_NAME_PARAM = "{" + SERVICE_NAME + "}";
    String ACCESS_URI = "accessUri";
    String ACCESS_URI_PARAM = "{" + ACCESS_URI + "}";

    String REGISTER_SERVICE_BINDING = "registerServiceBinding_" + SERVICE_NAME_PARAM + "_" + ACCESS_URI_PARAM;
    String UNREGISTER_SERVICE_BINDING = "unregisterServiceBinding_" + SERVICE_NAME_PARAM + "_" + ACCESS_URI_PARAM;
    String QUERY_SERVICE_BINDINGS = "queryServiceBindings_" + SERVICE_NAME_PARAM;
    String INFO = "info";
    String AVAILABLE = "available";
    String CLIENTIP = "clientip";

}
