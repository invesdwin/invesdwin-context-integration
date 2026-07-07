package net.sf.webdav.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * XMLWriter helper class.
 * 
 * @author <a href="mailto:remm@apache.org">Remy Maucherat</a>
 */
@NotThreadSafe
public class XMLWriter {

    // -------------------------------------------------------------- Constants

    /**
     * Opening tag.
     */
    public static final int OPENING = 0;

    /**
     * Closing tag.
     */
    public static final int CLOSING = 1;

    /**
     * Element with no content.
     */
    public static final int NO_CONTENT = 2;

    // ----------------------------------------------------- Instance Variables

    /**
     * Buffer.
     */
    protected StringBuffer buffer = new StringBuffer();

    /**
     * Writer.
     */
    protected Writer writer = null;

    /**
     * Namespaces to be declared in the root element
     */
    protected Map<String, String> namespaces;

    /**
     * Is true until the root element is written
     */
    protected boolean isRootElement = true;

    // ----------------------------------------------------------- Constructors

    /**
     * Constructor.
     */
    public XMLWriter(final Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    /**
     * Constructor.
     */
    public XMLWriter(final Writer writer, final Map<String, String> namespaces) {
        this.writer = writer;
        this.namespaces = namespaces;
    }

    // --------------------------------------------------------- Public Methods

    /**
     * Retrieve generated XML.
     * 
     * @return String containing the generated XML
     */
    @Override
    public String toString() {
        return this.buffer.toString();
    }

    /**
     * Write property to the XML.
     * 
     * @param name
     *            Property name
     * @param value
     *            Property value
     */
    public void writeProperty(final String name, final String value) {
        writeElement(name, OPENING);
        this.buffer.append(value);
        writeElement(name, CLOSING);
    }

    /**
     * Write property to the XML.
     * 
     * @param name
     *            Property name
     */
    public void writeProperty(final String name) {
        writeElement(name, NO_CONTENT);
    }

    /**
     * Write an element.
     * 
     * @param name
     *            Element name
     * @param type
     *            Element type
     */
    public void writeElement(final String pName, final int type) {
        final StringBuffer nsdecl = new StringBuffer();

        if (this.isRootElement) {
            for (final Iterator<String> iter = this.namespaces.keySet().iterator(); iter.hasNext();) {
                final String fullName = iter.next();
                final String abbrev = this.namespaces.get(fullName);
                nsdecl.append(" xmlns:").append(abbrev).append("=\"").append(fullName).append("\"");
            }
            this.isRootElement = false;
        }

        String name = pName;
        final int pos = name.lastIndexOf(':');
        if (pos >= 0) {
            // lookup prefix for namespace
            final String fullns = name.substring(0, pos);
            final String prefix = this.namespaces.get(fullns);
            if (prefix == null) {
                // there is no prefix for this namespace
                name = name.substring(pos + 1);
                nsdecl.append(" xmlns=\"").append(fullns).append("\"");
            } else {
                // there is a prefix
                name = prefix + ":" + name.substring(pos + 1);
            }
        } else {
            throw new IllegalArgumentException("All XML elements must have a namespace");
        }

        switch (type) {
        case OPENING:
            this.buffer.append("<");
            this.buffer.append(name);
            this.buffer.append(nsdecl);
            this.buffer.append(">");
            break;
        case CLOSING:
            this.buffer.append("</");
            this.buffer.append(name);
            this.buffer.append(">\n");
            break;
        case NO_CONTENT:
        default:
            this.buffer.append("<");
            this.buffer.append(name);
            this.buffer.append(nsdecl);
            this.buffer.append("/>");
            break;
        }
    }

    /**
     * Write text.
     * 
     * @param text
     *            Text to append
     */
    public void writeText(final String text) {
        this.buffer.append(text);
    }

    /**
     * Write data.
     * 
     * @param data
     *            Data to append
     */
    public void writeData(final String data) {
        this.buffer.append("<![CDATA[");
        this.buffer.append(data);
        this.buffer.append("]]>");
    }

    /**
     * Write XML Header.
     */
    public void writeXMLHeader() {
        this.buffer.append("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n");
    }

    /**
     * Send data and reinitializes buffer.
     */
    public void sendData() throws IOException {
        if (this.writer != null) {
            this.writer.write(this.buffer.toString());
            this.writer.flush();
            this.buffer = new StringBuffer();
        }
    }

}
