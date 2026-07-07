package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_METHOD_NOT_ALLOWED;
import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.IMimeTyper;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

@NotThreadSafe
public class DoGet extends DoHead {

    private static final int MAX_CACHE_SIZE = 1000;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoGet.class);

    //CHECKSTYLE:OFF
    private static final Map<Locale, DateFormat> DATE_FORMAT_CACHE = new ConcurrentHashMap<>();
    //CHECKSTYLE:ON

    public DoGet(final IWebdavStore store, final String dftIndexFile, final String insteadOf404,
            final ResourceLocks resourceLocks, final IMimeTyper mimeTyper, final boolean contentLengthHeader) {
        super(store, dftIndexFile, insteadOf404, resourceLocks, mimeTyper, contentLengthHeader);

    }

    @Override
    protected void doBody(final ITransaction transaction, final IWebdavResponse resp, final String path) {

        try {
            final StoredObject so = this.store.getStoredObject(transaction, path);
            if (so.isNullResource()) {
                final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(so);
                resp.addHeader("Allow", methodsAllowed);
                resp.sendError(SC_METHOD_NOT_ALLOWED);
                return;
            }
            final OutputStream out = resp.getOutputStream();
            final InputStream in = this.store.getResourceContent(transaction, path);
            try {
                int read = -1;
                final byte[] copyBuffer = new byte[BUF_SIZE];

                while ((read = in.read(copyBuffer, 0, copyBuffer.length)) != -1) {
                    out.write(copyBuffer, 0, read);
                }
            } finally {
                // flushing causes a IOE if a file is opened on the webserver
                // client disconnected before server finished sending response
                try {
                    in.close();
                } catch (final Exception e) {
                    LOG.warn("Closing InputStream causes Exception!\n" + e.toString());
                }
                try {
                    out.flush();
                    out.close();
                } catch (final Exception e) {
                    LOG.warn("Flushing OutputStream causes Exception!\n" + e.toString());
                }
            }
        } catch (final Exception e) {
            LOG.trace(e.toString());
        }
    }

    @Override
    protected void folderBody(final ITransaction transaction, final String path, final IWebdavResponse resp,
            final IWebdavRequest req) throws IOException, WebdavException {

        final StoredObject so = this.store.getStoredObject(transaction, path);
        if (so == null) {
            resp.sendError(SC_NOT_FOUND, req.getRequestURI());
        } else {

            if (so.isNullResource()) {
                final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(so);
                resp.addHeader("Allow", methodsAllowed);
                resp.sendError(SC_METHOD_NOT_ALLOWED);
                return;
            }

            if (so.isFolder()) {
                // TODO some folder response (for browsers, DAV tools
                // use propfind) in html?
                final DateFormat shortDF = getDateTimeFormat(req.getLocale());
                resp.setContentType("text/html");
                resp.setCharacterEncoding("UTF8");
                final OutputStream out = resp.getOutputStream();
                String[] children = this.store.getChildrenNames(transaction, path);
                children = children == null ? new String[] {} : children;

                // FIXME Use a content template for this!!
                final StringBuilder childrenTemp = new StringBuilder();
                childrenTemp.append("<html><head><title>Content of folder");
                childrenTemp.append(path);
                childrenTemp.append("</title><style type=\"text/css\">");
                childrenTemp.append(getCSS());
                childrenTemp.append("</style></head>");
                childrenTemp.append("<body>");
                childrenTemp.append(getHeader(transaction, path, resp, req));
                childrenTemp.append("<table>");
                childrenTemp.append("<tr><th>Name</th><th>Size</th><th>Created</th><th>Modified</th></tr>");
                childrenTemp.append("<tr>");
                childrenTemp.append("<td colspan=\"4\"><a href=\"../\">Parent</a></td></tr>");
                boolean isEven = false;
                for (final String child : children) {
                    isEven = !isEven;
                    childrenTemp.append("<tr class=\"");
                    childrenTemp.append(isEven ? "even" : "odd");
                    childrenTemp.append("\">");
                    childrenTemp.append("<td>");
                    childrenTemp.append("<a href=\"");
                    childrenTemp.append(child);
                    final StoredObject obj = this.store.getStoredObject(transaction, path + "/" + child);
                    if (obj.isFolder()) {
                        childrenTemp.append("/");
                    }
                    childrenTemp.append("\">");
                    childrenTemp.append(child);
                    childrenTemp.append("</a></td>");
                    if (obj.isFolder()) {
                        childrenTemp.append("<td>Folder</td>");
                    } else {
                        childrenTemp.append("<td>");
                        childrenTemp.append(obj.getResourceLength());
                        childrenTemp.append(" Bytes</td>");
                    }
                    if (obj.getCreationDate() != null) {
                        childrenTemp.append("<td>");
                        childrenTemp.append(shortDF.format(obj.getCreationDate()));
                        childrenTemp.append("</td>");
                    } else {
                        childrenTemp.append("<td></td>");
                    }
                    if (obj.getLastModified() != null) {
                        childrenTemp.append("<td>");
                        childrenTemp.append(shortDF.format(obj.getLastModified()));
                        childrenTemp.append("</td>");
                    } else {
                        childrenTemp.append("<td></td>");
                    }
                    childrenTemp.append("</tr>");
                }
                childrenTemp.append("</table>");
                childrenTemp.append(getFooter(transaction, path, resp, req));
                childrenTemp.append("</body></html>");
                out.write(childrenTemp.toString().getBytes("UTF-8"));
            }
        }
    }

    /**
     * Return the CSS styles used to display the HTML representation of the webdav content.
     * 
     * @return String returning the CSS style sheet used to display result in html format
     */
    protected String getCSS() {
        // The default styles to use
        String retVal = "body {\n" + "\tfont-family: Arial, Helvetica, sans-serif;\n" + "}\n" + "h1 {\n"
                + "\tfont-size: 1.5em;\n" + "}\n" + "th {\n" + "\tbackground-color: #9DACBF;\n" + "}\n" + "table {\n"
                + "\tborder-top-style: solid;\n" + "\tborder-right-style: solid;\n" + "\tborder-bottom-style: solid;\n"
                + "\tborder-left-style: solid;\n" + "}\n" + "td {\n" + "\tmargin: 0px;\n" + "\tpadding-top: 2px;\n"
                + "\tpadding-right: 5px;\n" + "\tpadding-bottom: 2px;\n" + "\tpadding-left: 5px;\n" + "}\n"
                + "tr.even {\n" + "\tbackground-color: #CCCCCC;\n" + "}\n" + "tr.odd {\n"
                + "\tbackground-color: #FFFFFF;\n" + "}\n" + "";
        try {
            // Try loading one via class loader and use that one instead
            final ClassLoader cl = getClass().getClassLoader();
            final InputStream iStream = cl.getResourceAsStream("webdav.css");
            if (iStream != null) {
                // Found css via class loader, use that one
                final StringBuilder out = new StringBuilder();
                final byte[] b = new byte[4096];
                //CHECKSTYLE:OFF
                for (int n; (n = iStream.read(b)) != -1;) {
                    //CHECKSTYLE:ON
                    out.append(new String(b, 0, n));
                }
                retVal = out.toString();
            }
        } catch (final Exception ex) {
            LOG.error("Error in reading webdav.css", ex);
        }

        return retVal;
    }

    /**
     * Return this as the Date/Time format for displaying Creation + Modification dates
     * 
     * @param browserLocale
     * @return DateFormat used to display creation and modification dates
     */
    protected DateFormat getDateTimeFormat(final Locale browserLocale) {
        if (DATE_FORMAT_CACHE.size() > MAX_CACHE_SIZE) {
            DATE_FORMAT_CACHE.clear();
        }
        return DATE_FORMAT_CACHE.computeIfAbsent(browserLocale, (l) -> {
            return java.text.SimpleDateFormat.getDateTimeInstance(java.text.SimpleDateFormat.SHORT,
                    java.text.SimpleDateFormat.MEDIUM, browserLocale);
        });
    }

    /**
     * Return the header to be displayed in front of the folder content
     * 
     * @param transaction
     * @param path
     * @param resp
     * @param req
     * @return String representing the header to be display in front of the folder content
     */
    protected String getHeader(final ITransaction transaction, final String path, final IWebdavResponse resp,
            final IWebdavRequest req) {
        return "<h1>Content of folder " + path + "</h1>";
    }

    /**
     * Return the footer to be displayed after the folder content
     * 
     * @param transaction
     * @param path
     * @param resp
     * @param req
     * @return String representing the footer to be displayed after the folder content
     */
    protected String getFooter(final ITransaction transaction, final String path, final IWebdavResponse resp,
            final IWebdavRequest req) {
        return "";
    }
}
