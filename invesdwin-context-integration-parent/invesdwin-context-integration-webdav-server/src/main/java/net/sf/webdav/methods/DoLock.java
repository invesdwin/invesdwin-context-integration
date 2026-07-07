package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_BAD_REQUEST;
import static net.sf.webdav.WebdavStatus.SC_CREATED;
import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_INTERNAL_SERVER_ERROR;
import static net.sf.webdav.WebdavStatus.SC_LOCKED;
import static net.sf.webdav.WebdavStatus.SC_NO_CONTENT;
import static net.sf.webdav.WebdavStatus.SC_OK;
import static net.sf.webdav.WebdavStatus.SC_PRECONDITION_FAILED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.invesdwin.util.error.Throwables;
import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.IResourceLocks;
import net.sf.webdav.locking.LockedObject;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;
import net.sf.webdav.util.XMLWriter;

@NotThreadSafe
public class DoLock extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoLock.class);

    private final IWebdavStore store;

    private final IResourceLocks resourceLocks;

    private final boolean readOnly;

    private boolean macLockRequest = false;

    private boolean exclusive = false;

    private String type = null;

    private String lockOwner = null;

    private String path = null;

    private String parentPath = null;

    private String userAgent = null;

    public DoLock(final IWebdavStore store, final IResourceLocks resourceLocks, final boolean readOnly) {
        this.store = store;
        this.resourceLocks = resourceLocks;
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, WebdavException {
        LOG.trace("-- " + this.getClass().getName());

        if (this.readOnly) {
            resp.sendError(SC_FORBIDDEN);
            return;
        } else {
            this.path = getRelativePath(req);
            this.parentPath = getParentPath(getCleanPath(this.path));

            final Hashtable<String, WebdavStatus> errorList = new Hashtable<String, WebdavStatus>();

            if (!checkLocks(transaction, req, resp, this.resourceLocks, this.path)) {
                errorList.put(this.path, SC_LOCKED);
                sendReport(req, resp, errorList);
                return; // resource is locked
            }

            if (!checkLocks(transaction, req, resp, this.resourceLocks, this.parentPath)) {
                errorList.put(this.parentPath, SC_LOCKED);
                sendReport(req, resp, errorList);
                return; // parent is locked
            }

            // Mac OS Finder (whether 10.4.x or 10.5) can't store files
            // because executing a LOCK without lock information causes a
            // SC_BAD_REQUEST
            this.userAgent = req.getHeader("User-Agent");
            if (this.userAgent != null && this.userAgent.indexOf("Darwin") != -1) {
                this.macLockRequest = true;

                //CHECKSTYLE:OFF
                final String timeString = Long.toString(System.currentTimeMillis());
                //CHECKSTYLE:ON
                this.lockOwner = this.userAgent.concat(timeString);
            }

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doLock" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON
            if (this.resourceLocks.lock(transaction, this.path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {
                    if (req.getHeader("If") != null) {
                        doRefreshLock(transaction, req, resp);
                    } else {
                        doLock(transaction, req, resp);
                    }
                } catch (final LockFailedException e) {
                    resp.sendError(SC_LOCKED);
                    LOG.warn(Throwables.getFullStackTrace(e));
                } finally {
                    this.resourceLocks.unlockTemporaryLockedObjects(transaction, this.path, tempLockOwner);
                }
            }
        }
    }

    private void doLock(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, WebdavException {

        StoredObject so = this.store.getStoredObject(transaction, this.path);

        if (so != null) {
            doLocking(transaction, req, resp);
        } else {
            // resource doesn't exist, null-resource lock
            doNullResourceLock(transaction, req, resp);
        }

        so = null;
        this.exclusive = false;
        this.type = null;
        this.lockOwner = null;

    }

    private void doLocking(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException {

        // Tests if LockObject on requested path exists, and if so, tests
        // exclusivity
        LockedObject lo = this.resourceLocks.getLockedObjectByPath(transaction, this.path);
        if (lo != null) {
            if (lo.isExclusive()) {
                sendLockFailError(transaction, req, resp);
                return;
            }
        }
        try {
            // Thats the locking itself
            executeLock(transaction, req, resp);

        } catch (final LockFailedException e) {
            sendLockFailError(transaction, req, resp);
        } catch (final WebdavException e) {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            LOG.trace(e.toString());
        } finally {
            lo = null;
        }

    }

    private void doNullResourceLock(final ITransaction transaction, final IWebdavRequest req,
            final IWebdavResponse resp) throws IOException {

        try {
            final StoredObject parentSo = this.store.getStoredObject(transaction, this.parentPath);
            if (this.parentPath != null && parentSo == null) {
                this.store.createFolder(transaction, this.parentPath);
            } else if (this.parentPath != null && parentSo != null && parentSo.isResource()) {
                resp.sendError(SC_PRECONDITION_FAILED);
                return;
            }

            StoredObject nullSo = this.store.getStoredObject(transaction, this.path);
            if (nullSo == null) {
                // resource doesn't exist
                this.store.createResource(transaction, this.path);

                // Transmit expects 204 response-code, not 201
                if (this.userAgent != null && this.userAgent.indexOf("Transmit") != -1) {
                    LOG.trace("DoLock.execute() : do workaround for user agent '" + this.userAgent + "'");
                    resp.setStatus(SC_NO_CONTENT);
                } else {
                    resp.setStatus(SC_CREATED);
                }

            } else {
                // resource already exists, could not execute null-resource lock
                sendLockFailError(transaction, req, resp);
                return;
            }
            nullSo = this.store.getStoredObject(transaction, this.path);
            // define the newly created resource as null-resource
            nullSo.setNullResource(true);

            // Thats the locking itself
            executeLock(transaction, req, resp);

        } catch (final LockFailedException e) {
            sendLockFailError(transaction, req, resp);
        } catch (final WebdavException e) {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            LOG.warn(Throwables.getFullStackTrace(e));
        }
    }

    private void doRefreshLock(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {

        final String[] lockTokens = getLockIdFromIfHeader(req);
        String lockToken = null;
        if (lockTokens != null) {
            lockToken = lockTokens[0];
        }

        if (lockToken != null) {
            // Getting LockObject of specified lockToken in If header
            LockedObject refreshLo = this.resourceLocks.getLockedObjectByID(transaction, lockToken);
            if (refreshLo != null) {
                final int timeout = getTimeout(transaction, req);

                refreshLo.refreshTimeout(timeout);
                // sending success response
                generateXMLReport(transaction, resp, refreshLo);

                refreshLo = null;
            } else {
                // no LockObject to given lockToken
                resp.sendError(SC_PRECONDITION_FAILED);
            }

        } else {
            resp.sendError(SC_PRECONDITION_FAILED);
        }
    }

    // ------------------------------------------------- helper methods

    /**
     * Executes the LOCK
     */
    private void executeLock(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws LockFailedException, IOException, WebdavException {

        // Mac OS lock request workaround
        if (this.macLockRequest) {
            LOG.trace("DoLock.execute() : do workaround for user agent '" + this.userAgent + "'");

            doMacLockRequestWorkaround(transaction, req, resp);
        } else {
            // Getting LockInformation from request
            if (getLockInformation(transaction, req, resp)) {
                final int depth = getDepth(req);
                final int lockDuration = getTimeout(transaction, req);

                boolean lockSuccess = false;
                if (this.exclusive) {
                    lockSuccess = this.resourceLocks.exclusiveLock(transaction, this.path, this.lockOwner, depth,
                            lockDuration);
                } else {
                    lockSuccess = this.resourceLocks.sharedLock(transaction, this.path, this.lockOwner, depth,
                            lockDuration);
                }

                if (lockSuccess) {
                    // Locks successfully placed - return information about
                    final LockedObject lo = this.resourceLocks.getLockedObjectByPath(transaction, this.path);
                    if (lo != null) {
                        generateXMLReport(transaction, resp, lo);
                    } else {
                        resp.sendError(SC_INTERNAL_SERVER_ERROR);
                    }
                } else {
                    sendLockFailError(transaction, req, resp);

                    throw new LockFailedException();
                }
            } else {
                // information for LOCK could not be read successfully
                resp.setContentType("text/xml; charset=UTF-8");
                resp.sendError(SC_BAD_REQUEST);
            }
        }
    }

    /**
     * Tries to get the LockInformation from LOCK request
     */
    //CHECKSTYLE:OFF
    private boolean getLockInformation(final ITransaction transaction, final IWebdavRequest req,
            final IWebdavResponse resp) throws WebdavException, IOException {
        //CHECKSTYLE:ON

        Node lockInfoNode = null;
        DocumentBuilder documentBuilder = null;

        documentBuilder = getDocumentBuilder();
        try {
            final Document document = documentBuilder.parse(new InputSource(req.getInputStream()));

            // Get the root element of the document
            final Element rootElement = document.getDocumentElement();

            lockInfoNode = rootElement;

            if (lockInfoNode != null) {
                NodeList childList = lockInfoNode.getChildNodes();
                Node lockScopeNode = null;
                Node lockTypeNode = null;
                Node lockOwnerNode = null;

                Node currentNode = null;
                String nodeName = null;

                for (int i = 0; i < childList.getLength(); i++) {
                    currentNode = childList.item(i);

                    if (currentNode.getNodeType() == Node.ELEMENT_NODE || currentNode.getNodeType() == Node.TEXT_NODE) {

                        nodeName = currentNode.getNodeName();

                        if (nodeName.endsWith("locktype")) {
                            lockTypeNode = currentNode;
                        }
                        if (nodeName.endsWith("lockscope")) {
                            lockScopeNode = currentNode;
                        }
                        if (nodeName.endsWith("owner")) {
                            lockOwnerNode = currentNode;
                        }
                    } else {
                        return false;
                    }
                }

                if (lockScopeNode != null) {
                    String scope = null;
                    childList = lockScopeNode.getChildNodes();
                    for (int i = 0; i < childList.getLength(); i++) {
                        currentNode = childList.item(i);

                        if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                            scope = currentNode.getNodeName();

                            if (scope.endsWith("exclusive")) {
                                this.exclusive = true;
                            } else if ("shared".equals(scope)) {
                                this.exclusive = false;
                            }
                        }
                    }
                    if (scope == null) {
                        return false;
                    }

                } else {
                    return false;
                }

                if (lockTypeNode != null) {
                    childList = lockTypeNode.getChildNodes();
                    for (int i = 0; i < childList.getLength(); i++) {
                        currentNode = childList.item(i);

                        if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                            this.type = currentNode.getNodeName();

                            if (this.type.endsWith("write")) {
                                this.type = "write";
                            } else if ("read".equals(this.type)) {
                                this.type = "read";
                            }
                        }
                    }
                    if (this.type == null) {
                        return false;
                    }
                } else {
                    return false;
                }

                if (lockOwnerNode != null) {
                    childList = lockOwnerNode.getChildNodes();
                    for (int i = 0; i < childList.getLength(); i++) {
                        currentNode = childList.item(i);

                        if (currentNode.getNodeType() == Node.ELEMENT_NODE
                                || currentNode.getNodeType() == Node.TEXT_NODE) {
                            this.lockOwner = currentNode.getTextContent();
                        }
                    }
                }
                if (this.lockOwner == null) {
                    return false;
                }
            } else {
                return false;
            }

        } catch (final DOMException e) {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            LOG.warn(Throwables.getFullStackTrace(e));
            return false;
        } catch (final SAXException e) {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            LOG.warn(Throwables.getFullStackTrace(e));
            return false;
        }

        return true;
    }

    /**
     * Ties to read the timeout from request
     */
    private int getTimeout(final ITransaction transaction, final IWebdavRequest req) {

        int lockDuration = DEFAULT_TIMEOUT;
        String lockDurationStr = req.getHeader("Timeout");

        if (lockDurationStr == null) {
            lockDuration = DEFAULT_TIMEOUT;
        } else {
            final int commaPos = lockDurationStr.indexOf(',');
            // if multiple timeouts, just use the first one
            if (commaPos != -1) {
                lockDurationStr = lockDurationStr.substring(0, commaPos);
            }
            if (lockDurationStr.startsWith("Second-")) {
                lockDuration = Integer.valueOf(lockDurationStr.substring(7));
            } else {
                if ("infinity".equalsIgnoreCase(lockDurationStr)) {
                    lockDuration = MAX_TIMEOUT;
                } else {
                    try {
                        lockDuration = Integer.valueOf(lockDurationStr);
                    } catch (final NumberFormatException e) {
                        lockDuration = MAX_TIMEOUT;
                    }
                }
            }
            if (lockDuration <= 0) {
                lockDuration = DEFAULT_TIMEOUT;
            }
            if (lockDuration > MAX_TIMEOUT) {
                lockDuration = MAX_TIMEOUT;
            }
        }
        return lockDuration;
    }

    /**
     * Generates the response XML with all lock information
     */
    private void generateXMLReport(final ITransaction transaction, final IWebdavResponse resp, final LockedObject lo)
            throws IOException {

        //CHECKSTYLE:OFF
        final HashMap<String, String> namespaces = new HashMap<String, String>();
        //CHECKSTYLE:ON
        namespaces.put("DAV:", "D");

        resp.setStatus(SC_OK);
        resp.setContentType("text/xml; charset=UTF-8");

        final XMLWriter generatedXML = new XMLWriter(resp.getWriter(), namespaces);
        generatedXML.writeXMLHeader();
        generatedXML.writeElement("DAV::prop", XMLWriter.OPENING);
        generatedXML.writeElement("DAV::lockdiscovery", XMLWriter.OPENING);
        generatedXML.writeElement("DAV::activelock", XMLWriter.OPENING);

        generatedXML.writeElement("DAV::locktype", XMLWriter.OPENING);
        generatedXML.writeProperty("DAV::" + this.type);
        generatedXML.writeElement("DAV::locktype", XMLWriter.CLOSING);

        generatedXML.writeElement("DAV::lockscope", XMLWriter.OPENING);
        if (this.exclusive) {
            generatedXML.writeProperty("DAV::exclusive");
        } else {
            generatedXML.writeProperty("DAV::shared");
        }
        generatedXML.writeElement("DAV::lockscope", XMLWriter.CLOSING);

        final int depth = lo.getLockDepth();

        generatedXML.writeElement("DAV::depth", XMLWriter.OPENING);
        if (depth == INFINITY) {
            generatedXML.writeText("Infinity");
        } else {
            generatedXML.writeText(String.valueOf(depth));
        }
        generatedXML.writeElement("DAV::depth", XMLWriter.CLOSING);

        generatedXML.writeElement("DAV::owner", XMLWriter.OPENING);
        generatedXML.writeElement("DAV::href", XMLWriter.OPENING);
        generatedXML.writeText(this.lockOwner);
        generatedXML.writeElement("DAV::href", XMLWriter.CLOSING);
        generatedXML.writeElement("DAV::owner", XMLWriter.CLOSING);

        final long timeout = lo.getTimeoutMillis();
        generatedXML.writeElement("DAV::timeout", XMLWriter.OPENING);
        generatedXML.writeText("Second-" + timeout / 1000);
        generatedXML.writeElement("DAV::timeout", XMLWriter.CLOSING);

        final String lockToken = lo.getID();
        generatedXML.writeElement("DAV::locktoken", XMLWriter.OPENING);
        generatedXML.writeElement("DAV::href", XMLWriter.OPENING);
        generatedXML.writeText("opaquelocktoken:" + lockToken);
        generatedXML.writeElement("DAV::href", XMLWriter.CLOSING);
        generatedXML.writeElement("DAV::locktoken", XMLWriter.CLOSING);

        generatedXML.writeElement("DAV::activelock", XMLWriter.CLOSING);
        generatedXML.writeElement("DAV::lockdiscovery", XMLWriter.CLOSING);
        generatedXML.writeElement("DAV::prop", XMLWriter.CLOSING);

        resp.addHeader("Lock-Token", "<opaquelocktoken:" + lockToken + ">");

        generatedXML.sendData();

    }

    /**
     * Executes the lock for a Mac OS Finder client
     */
    private void doMacLockRequestWorkaround(final ITransaction transaction, final IWebdavRequest req,
            final IWebdavResponse resp) throws LockFailedException, IOException {
        final LockedObject lo;
        final int depth = getDepth(req);
        int lockDuration = getTimeout(transaction, req);
        if (lockDuration < 0 || lockDuration > MAX_TIMEOUT) {
            lockDuration = DEFAULT_TIMEOUT;
        }

        boolean lockSuccess = false;
        lockSuccess = this.resourceLocks.exclusiveLock(transaction, this.path, this.lockOwner, depth, lockDuration);

        if (lockSuccess) {
            // Locks successfully placed - return information about
            lo = this.resourceLocks.getLockedObjectByPath(transaction, this.path);
            if (lo != null) {
                generateXMLReport(transaction, resp, lo);
            } else {
                resp.sendError(SC_INTERNAL_SERVER_ERROR);
            }
        } else {
            // Locking was not successful
            sendLockFailError(transaction, req, resp);
        }
    }

    /**
     * Sends an error report to the client
     */
    private void sendLockFailError(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException {
        final Hashtable<String, WebdavStatus> errorList = new Hashtable<String, WebdavStatus>();
        errorList.put(this.path, SC_LOCKED);
        sendReport(req, resp, errorList);
    }

}
