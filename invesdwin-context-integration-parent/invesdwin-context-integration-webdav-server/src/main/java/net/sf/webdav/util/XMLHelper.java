package net.sf.webdav.util;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Immutable
public final class XMLHelper {

    private XMLHelper() {}

    public static Node findSubElement(final Node parent, final String localName) {
        if (parent == null) {
            return null;
        }
        Node child = parent.getFirstChild();
        while (child != null) {
            if ((child.getNodeType() == Node.ELEMENT_NODE) && (child.getLocalName().equals(localName))) {
                return child;
            }
            child = child.getNextSibling();
        }
        return null;
    }

    public static List<String> getPropertiesFromXML(final Node propNode) {
        final ArrayList<String> properties = new ArrayList<String>();
        final NodeList childList = propNode.getChildNodes();

        for (int i = 0; i < childList.getLength(); i++) {
            final Node currentNode = childList.item(i);
            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                final String nodeName = currentNode.getLocalName();
                final String namespace = currentNode.getNamespaceURI();
                // href is a live property which is handled differently
                properties.add(namespace + ":" + nodeName);
            }
        }
        return properties;
    }

}
