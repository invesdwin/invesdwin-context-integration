package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_INTERNAL_SERVER_ERROR;
import static net.sf.webdav.WebdavStatus.SC_LOCKED;
import static net.sf.webdav.WebdavStatus.SC_METHOD_NOT_ALLOWED;
import static net.sf.webdav.WebdavStatus.SC_MULTI_STATUS;
import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;
import static net.sf.webdav.WebdavStatus.SC_OK;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.LockedObject;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;
import net.sf.webdav.util.XMLHelper;
import net.sf.webdav.util.XMLWriter;

@NotThreadSafe
public class DoProppatch extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoProppatch.class);

    private final boolean readOnly;

    private final IWebdavStore store;

    private final ResourceLocks resourceLocks;

    public DoProppatch(final IWebdavStore store, final ResourceLocks resLocks, final boolean readOnly) {
        this.readOnly = readOnly;
        this.store = store;
        this.resourceLocks = resLocks;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {
        LOG.trace("-- " + this.getClass().getName());

        if (this.readOnly) {
            resp.sendError(SC_FORBIDDEN);
            return;
        }

        final String path = getRelativePath(req);
        final String parentPath = getParentPath(getCleanPath(path));

        final Hashtable<String, WebdavStatus> errorList = new Hashtable<String, WebdavStatus>();

        if (!checkLocks(transaction, req, resp, this.resourceLocks, parentPath)) {
            errorList.put(parentPath, SC_LOCKED);
            sendReport(req, resp, errorList);
            return; // parent is locked
        }

        if (!checkLocks(transaction, req, resp, this.resourceLocks, path)) {
            errorList.put(path, SC_LOCKED);
            sendReport(req, resp, errorList);
            return; // resource is locked
        }

        // TODO for now, PROPPATCH just sends a valid response, stating that
        // everything is fine, but doesn't do anything.

        // Retrieve the resources
        //CHECKSTYLE:OFF
        final String tempLockOwner = "doProppatch" + System.currentTimeMillis() + req.toString();
        //CHECKSTYLE:ON

        if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
            try {
                executeLocked(transaction, req, resp, path);
            } catch (final AccessDeniedException e) {
                resp.sendError(SC_FORBIDDEN);
            } catch (final WebdavException e) {
                resp.sendError(SC_INTERNAL_SERVER_ERROR);
            } finally {
                this.resourceLocks.unlockTemporaryLockedObjects(transaction, path, tempLockOwner);
            }
        } else {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
        }
    }

    private void executeLocked(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp,
            final String pPath) throws WebdavException, IOException {
        String path = pPath;
        final Hashtable<String, WebdavStatus> errorList;
        final StoredObject so;
        final LockedObject lo;
        so = this.store.getStoredObject(transaction, path);
        lo = this.resourceLocks.getLockedObjectByPath(transaction, getCleanPath(path));

        if (so == null) {
            resp.sendError(SC_NOT_FOUND);
            return;
            // we do not to continue since there is no root
            // resource
        }

        if (so.isNullResource()) {
            final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(so);
            resp.addHeader("Allow", methodsAllowed);
            resp.sendError(SC_METHOD_NOT_ALLOWED);
            return;
        }

        if (lo != null && lo.isExclusive()) {
            // Object on specified path is LOCKED
            errorList = new Hashtable<String, WebdavStatus>();
            errorList.put(path, SC_LOCKED);
            sendReport(req, resp, errorList);
            return;
        }

        final List<String> tochange = new Vector<String>();
        // contains all properties from
        // toset and toremove

        path = getCleanPath(getRelativePath(req));

        Node tosetNode = null;
        Node toremoveNode = null;

        if (req.getContentLength() != 0) {
            final DocumentBuilder documentBuilder = getDocumentBuilder();
            try {
                final Document document = documentBuilder.parse(new InputSource(req.getInputStream()));
                // Get the root element of the document
                final Element rootElement = document.getDocumentElement();

                tosetNode = XMLHelper.findSubElement(XMLHelper.findSubElement(rootElement, "set"), "prop");
                toremoveNode = XMLHelper.findSubElement(XMLHelper.findSubElement(rootElement, "remove"), "prop");
            } catch (final Exception e) {
                resp.sendError(SC_INTERNAL_SERVER_ERROR);
                return;
            }
        } else {
            // no content: error
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            return;
        }

        generateXml(req, resp, path, so, tochange, tosetNode, toremoveNode);
    }

    private void generateXml(final IWebdavRequest req, final IWebdavResponse resp, final String path,
            final StoredObject so, final List<String> tochange, final Node tosetNode, final Node toremoveNode)
            throws IOException {
        final List<String> toset;
        final List<String> toremove;
        //CHECKSTYLE:OFF
        final HashMap<String, String> namespaces = new HashMap<String, String>();
        //CHECKSTYLE:ON
        namespaces.put("DAV:", "D");

        if (tosetNode != null) {
            toset = XMLHelper.getPropertiesFromXML(tosetNode);
            tochange.addAll(toset);
        }

        if (toremoveNode != null) {
            toremove = XMLHelper.getPropertiesFromXML(toremoveNode);
            tochange.addAll(toremove);
        }

        resp.setStatus(SC_MULTI_STATUS);
        resp.setContentType("text/xml; charset=UTF-8");

        // Create multistatus object
        final XMLWriter generatedXML = new XMLWriter(resp.getWriter(), namespaces);
        generatedXML.writeXMLHeader();
        generatedXML.writeElement("DAV::multistatus", XMLWriter.OPENING);

        generatedXML.writeElement("DAV::response", XMLWriter.OPENING);
        final String status = new String("HTTP/1.1 " + SC_OK + " " + SC_OK.message());

        // Generating href element
        generatedXML.writeElement("DAV::href", XMLWriter.OPENING);

        String href = req.getContextPath();
        if ((href.endsWith("/")) && (path.startsWith("/"))) {
            href += path.substring(1);
        } else {
            href += path;
        }
        if ((so.isFolder()) && (!href.endsWith("/"))) {
            href += "/";
        }

        generatedXML.writeText(rewriteUrl(href));

        generatedXML.writeElement("DAV::href", XMLWriter.CLOSING);

        for (final String string : tochange) {
            final String property = string;

            generatedXML.writeElement("DAV::propstat", XMLWriter.OPENING);

            generatedXML.writeElement("DAV::prop", XMLWriter.OPENING);
            generatedXML.writeElement(property, XMLWriter.NO_CONTENT);
            generatedXML.writeElement("DAV::prop", XMLWriter.CLOSING);

            generatedXML.writeElement("DAV::status", XMLWriter.OPENING);
            generatedXML.writeText(status);
            generatedXML.writeElement("DAV::status", XMLWriter.CLOSING);

            generatedXML.writeElement("DAV::propstat", XMLWriter.CLOSING);
        }

        generatedXML.writeElement("DAV::response", XMLWriter.CLOSING);

        generatedXML.writeElement("DAV::multistatus", XMLWriter.CLOSING);

        generatedXML.sendData();
    }
}
