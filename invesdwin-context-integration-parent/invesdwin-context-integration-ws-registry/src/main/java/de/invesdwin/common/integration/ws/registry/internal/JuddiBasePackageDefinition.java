package de.invesdwin.common.integration.ws.registry.internal;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import de.invesdwin.context.beans.init.locations.IBasePackageDefinition;

@Immutable
@Named
public class JuddiBasePackageDefinition implements IBasePackageDefinition {

    @Override
    public String getBasePackage() {
        return "org.apache.juddi";
    }

}
