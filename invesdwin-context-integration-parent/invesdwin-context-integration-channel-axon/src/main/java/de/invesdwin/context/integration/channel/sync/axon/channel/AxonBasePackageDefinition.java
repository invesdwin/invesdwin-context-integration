package de.invesdwin.context.integration.channel.sync.axon.channel;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.IBasePackageDefinition;

@Immutable
public class AxonBasePackageDefinition implements IBasePackageDefinition {

    @Override
    public String getBasePackage() {
        return "org.axonframework.eventsourcing.eventstore.jpa";
    }

}
