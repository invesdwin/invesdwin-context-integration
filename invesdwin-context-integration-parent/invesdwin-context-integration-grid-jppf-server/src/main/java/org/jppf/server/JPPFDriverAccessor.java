package org.jppf.server;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.jppf.server.node.JPPFNode;

@Immutable
public final class JPPFDriverAccessor {

    private JPPFDriverAccessor() {}

    public static List<JPPFNode> getLocalNodes(final JPPFDriver driver) {
        return driver.localNodes;
    }

}
