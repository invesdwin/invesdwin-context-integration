package de.invesdwin.context.integration.jms.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

@NotThreadSafe
public class ConfirguredEmbeddedActiveMQ extends EmbeddedActiveMQ {

    public ConfirguredEmbeddedActiveMQ() {
        final Configuration config = new ConfigurationImpl();
        try {
            config.addAcceptorConfiguration("vm", "vm://default");
            config.addAcceptorConfiguration("tcp", "tcp://localhost:0");
            config.addConnectorConfiguration("vm", "vm://default");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        final DiscoveryGroupConfiguration discovery = new DiscoveryGroupConfiguration();
        final UDPBroadcastEndpointFactory udpBroadcast = new UDPBroadcastEndpointFactory();
        discovery.setBroadcastEndpointFactory(udpBroadcast);
        config.addDiscoveryGroupConfiguration("default", discovery);
        config.setPersistenceEnabled(false);
        config.setSecurityEnabled(false);

        //https://developers.redhat.com/articles/2021/06/30/implementing-apache-activemq-style-broker-meshes-apache-artemis#
        final ClusterConnectionConfiguration clusterConfig = new ClusterConnectionConfiguration();
        clusterConfig.setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND);
        config.addClusterConfiguration(clusterConfig);
        final AddressSettings addressesSetting = new AddressSettings();
        addressesSetting.setRedistributionDelay(1000);
        config.addAddressSetting("default", addressesSetting);
        setConfiguration(config);
    }

}
