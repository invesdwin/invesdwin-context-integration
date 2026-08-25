package org.apache.catalina.servlets;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.catalina.servlets.DefaultServlet.SortManager.Order;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@ThreadSafe
public class ExtendedWebdavServlet extends WebdavServlet {

    private static final MethodHandle RESOURCE_LOCKS_GETTER;
    private static final MethodHandle STORE_GETTER;
    private static final MethodHandle IS_LOCKED_METHOD;
    private static final MethodHandle IS_SPECIAL_PATH_METHOD;
    private static final MethodHandle GET_ENCODED_PATH_METHOD;

    private static final MethodHandle LOCK_INFO_PATH_SETTER;
    private static final MethodHandle LOCK_INFO_LOCKROOT_SETTER;

    private static final MethodHandle PROPERTY_STORE_COPY_METHOD;
    private static final MethodHandle PROPERTY_STORE_DELETE_METHOD;

    private static final MethodHandle GET_ORDER_CHAR_METHOD;

    static {
        try {
            final MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(WebdavServlet.class,
                    MethodHandles.lookup());

            // Fields
            final Field locksField = WebdavServlet.class.getDeclaredField("resourceLocks");
            RESOURCE_LOCKS_GETTER = lookup.unreflectGetter(locksField);

            final Field storeField = WebdavServlet.class.getDeclaredField("store");
            STORE_GETTER = lookup.unreflectGetter(storeField);

            // Methods
            final Method isLockedMethod = WebdavServlet.class.getDeclaredMethod("isLocked", String.class,
                    HttpServletRequest.class);
            IS_LOCKED_METHOD = lookup.unreflect(isLockedMethod);

            final Method isSpecialPathMethod = WebdavServlet.class.getDeclaredMethod("isSpecialPath", String.class);
            IS_SPECIAL_PATH_METHOD = lookup.unreflect(isSpecialPathMethod);

            final Method getEncodedPathMethod = WebdavServlet.class.getDeclaredMethod("getEncodedPath", String.class,
                    org.apache.catalina.WebResource.class, HttpServletRequest.class);
            GET_ENCODED_PATH_METHOD = lookup.unreflect(getEncodedPathMethod);

            // LockInfo inner class fields
            Class<?> lockInfoClass = null;
            for (final Class<?> declaredClass : WebdavServlet.class.getDeclaredClasses()) {
                if (declaredClass.getSimpleName().equals("LockInfo")) {
                    lockInfoClass = declaredClass;
                    break;
                }
            }
            if (lockInfoClass == null) {
                lockInfoClass = Class.forName("org.apache.catalina.servlets.WebdavServlet$LockInfo");
            }

            final MethodHandles.Lookup privateLockInfoLookup = MethodHandles.privateLookupIn(lockInfoClass,
                    MethodHandles.lookup());
            final Field pathField = lockInfoClass.getDeclaredField("path");
            final Field lockRootField = lockInfoClass.getDeclaredField("lockroot");

            LOCK_INFO_PATH_SETTER = privateLockInfoLookup.unreflectSetter(pathField);
            LOCK_INFO_LOCKROOT_SETTER = privateLockInfoLookup.unreflectSetter(lockRootField);

            // PropertyStore inner interface / class methods
            Class<?> propertyStoreClass = null;
            for (final Class<?> declaredClass : WebdavServlet.class.getDeclaredClasses()) {
                if (declaredClass.getSimpleName().equals("PropertyStore")) {
                    propertyStoreClass = declaredClass;
                    break;
                }
            }
            if (propertyStoreClass == null) {
                propertyStoreClass = Class.forName("org.apache.catalina.servlets.WebdavServlet$PropertyStore");
            }

            final MethodHandles.Lookup privatePropertyStoreLookup = MethodHandles.privateLookupIn(propertyStoreClass,
                    MethodHandles.lookup());
            final Method copyMethod = propertyStoreClass.getDeclaredMethod("copy", String.class, String.class);
            PROPERTY_STORE_COPY_METHOD = privatePropertyStoreLookup.unreflect(copyMethod);

            final Method deleteMethod = propertyStoreClass.getDeclaredMethod("delete", String.class);
            PROPERTY_STORE_DELETE_METHOD = privatePropertyStoreLookup.unreflect(deleteMethod);

            // DefaultServlet method handles
            final MethodHandles.Lookup lookupDefault = MethodHandles.privateLookupIn(DefaultServlet.class,
                    MethodHandles.lookup());
            final Method getOrderCharMethod = DefaultServlet.class.getDeclaredMethod("getOrderChar", Order.class,
                    char.class);
            GET_ORDER_CHAR_METHOD = lookupDefault.unreflect(getOrderCharMethod);

        } catch (final Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, Object> getResourceLocks() {
        try {
            return (ConcurrentHashMap<String, Object>) RESOURCE_LOCKS_GETTER.invoke(this);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private Object getStore() {
        try {
            return STORE_GETTER.invoke(this);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private boolean checkIsLocked(final String path, final HttpServletRequest req) {
        try {
            return (boolean) IS_LOCKED_METHOD.invoke(this, path, req);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private boolean checkIsSpecialPath(final String path) {
        try {
            return (boolean) IS_SPECIAL_PATH_METHOD.invoke(this, path);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private String invokeGetEncodedPath(final String path, final org.apache.catalina.WebResource resource,
            final HttpServletRequest req) {
        try {
            return (String) GET_ENCODED_PATH_METHOD.invoke(this, path, resource, req);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private char invokeGetOrderChar(final Order order, final char column) {
        try {
            return (char) GET_ORDER_CHAR_METHOD.invoke(this, order, column);
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    //CHECKSTYLE:OFF
    @Override
    protected void doMove(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        //CHECKSTYLE:ON
        if (isReadOnly()) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
            return;
        }

        final String path = getRelativePath(req);

        if (checkIsLocked(path, req)) {
            resp.sendError(WebdavStatus.SC_LOCKED);
            return;
        }

        // Verify source resource exists and is a single file (not a collection)
        final org.apache.catalina.WebResource sourceResource = resources.getResource(path);
        if (!sourceResource.exists() || sourceResource.isDirectory()) {
            super.doMove(req, resp);
            return;
        }

        // Parse and validate the Destination header
        final String destinationHeader = req.getHeader("Destination");
        if (destinationHeader == null || destinationHeader.isEmpty()) {
            resp.sendError(WebdavStatus.SC_BAD_REQUEST);
            return;
        }

        final URI destinationUri;
        try {
            destinationUri = new java.net.URI(destinationHeader);
        } catch (final java.net.URISyntaxException e) {
            resp.sendError(WebdavStatus.SC_BAD_REQUEST);
            return;
        }

        String destinationPath = destinationUri.getPath();
        if (destinationPath == null
                || !destinationPath.equals(org.apache.tomcat.util.http.RequestUtil.normalize(destinationPath))) {
            resp.sendError(WebdavStatus.SC_BAD_REQUEST);
            return;
        }

        final String reqContextPath = getPathPrefix(req);
        if (!destinationPath.startsWith(reqContextPath + "/")) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
            return;
        }
        destinationPath = destinationPath.substring(reqContextPath.length());

        if (checkIsSpecialPath(destinationPath) || destinationPath.equals(path)) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
            return;
        }

        if (checkIsLocked(destinationPath, req)) {
            resp.sendError(WebdavStatus.SC_LOCKED);
            return;
        }

        boolean overwrite = true;
        final String overwriteHeader = req.getHeader("Overwrite");
        if (overwriteHeader != null) {
            overwrite = "T".equalsIgnoreCase(overwriteHeader);
        }

        final org.apache.catalina.WebResource destinationResource = resources.getResource(destinationPath);
        if (destinationResource.exists()) {
            if (!overwrite) {
                resp.sendError(WebdavStatus.SC_PRECONDITION_FAILED);
                return;
            }
            if (destinationResource.isDirectory()) {
                resp.sendError(WebdavStatus.SC_METHOD_NOT_ALLOWED);
                return;
            }
        }

        // Perform atomic move via the resources provider
        final FakeCatalinaWebdavResourceRoot cResources = (FakeCatalinaWebdavResourceRoot) this.resources;
        final boolean success = cResources.move(path, destinationPath);
        if (!success) {
            resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        // Transfer the lock mapping using MethodHandles
        final ConcurrentHashMap<String, Object> locks = getResourceLocks();
        if (locks != null) {
            final Object lockInfo = locks.remove(path);
            if (lockInfo != null) {
                try {
                    LOCK_INFO_PATH_SETTER.invoke(lockInfo, destinationPath);
                    LOCK_INFO_LOCKROOT_SETTER.invoke(lockInfo,
                            invokeGetEncodedPath(destinationPath, destinationResource, req));
                } catch (final Throwable t) {
                    throw new IOException("Failed to update lock info paths via MethodHandle", t);
                }
                locks.put(destinationPath, lockInfo);
            }
        }

        // Handle dead properties via store using MethodHandles
        final Object propertyStore = getStore();
        if (propertyStore != null) {
            try {
                PROPERTY_STORE_COPY_METHOD.invoke(propertyStore, path, destinationPath);
                PROPERTY_STORE_DELETE_METHOD.invoke(propertyStore, path);
            } catch (final Throwable t) {
                if (t instanceof IOException) {
                    throw (IOException) t;
                }
                throw new IOException("Failed to update PropertyStore dead properties for path: " + path, t);
            }
        }

        if (destinationResource.exists()) {
            resp.setStatus(WebdavStatus.SC_NO_CONTENT);
        } else {
            resp.setStatus(WebdavStatus.SC_CREATED);
        }
    }

    //CHECKSTYLE:OFF
    @Override
    protected InputStream renderHtml(final HttpServletRequest request, final String contextPath,
            final org.apache.catalina.WebResource resource, final String encoding) throws IOException {
        //CHECKSTYLE:ON

        // Prepare a writer to a buffered area
        final java.io.ByteArrayOutputStream stream = new java.io.ByteArrayOutputStream();
        final java.io.OutputStreamWriter osWriter = new java.io.OutputStreamWriter(stream,
                java.nio.charset.StandardCharsets.UTF_8);
        final java.io.PrintWriter writer = new java.io.PrintWriter(osWriter);

        final StringBuilder sb = new StringBuilder();

        // Get the right strings
        final org.apache.tomcat.util.res.StringManager sm = org.apache.tomcat.util.res.StringManager
                .getManager(org.apache.catalina.servlets.DefaultServlet.class.getPackageName(), request.getLocales());

        final String directoryWebappPath = resource.getWebappPath();
        final String escapedDirectoryWebappPath = org.apache.tomcat.util.security.Escape
                .htmlElementContent(directoryWebappPath);
        final org.apache.catalina.WebResource[] entries = resources.listResources(directoryWebappPath);

        // rewriteUrl(contextPath) is expensive. cache result for later reuse
        final String rewrittenContextPath = rewriteUrl(contextPath);

        // Render the page header
        sb.append("<!doctype html>\r\n");
        sb.append("<html lang=\"").append(sm.getLocale().getLanguage()).append("\">\r\n");
        sb.append("<head>\r\n");
        sb.append("<title>")
                .append(sm.getString("defaultServlet.directory.title", escapedDirectoryWebappPath))
                .append("</title>\r\n");

        // Inject Custom Modern CSS
        sb.append("<style>\r\n");
        sb.append(
                ":root { --primary-color: #0066cc; --bg-color: #f4f6f9; --card-bg: #ffffff; --text-main: #333333; --text-muted: #666666; --border-color: #e1e4e8; --hover-bg: #f8f9fa; }\n");
        sb.append(
                "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: var(--text-main); background-color: var(--bg-color); margin: 0; padding: 2rem; }\n");
        sb.append(
                ".container { max-width: 1000px; margin: 0 auto; background: var(--card-bg); border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05), 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }\n");
        sb.append(
                ".header-bar { padding: 1.5rem 2rem; background-color: var(--card-bg); border-bottom: 1px solid var(--border-color); }\n");
        sb.append("h1 { margin: 0; font-size: 1.25rem; font-weight: 600; word-break: break-all; }\n");
        sb.append("table { width: 100%; border-collapse: collapse; text-align: left; }\n");
        sb.append("th, td { padding: 0.85rem 2rem; font-size: 0.9rem; }\n");
        sb.append(
                "th { background-color: var(--hover-bg); color: var(--text-muted); font-weight: 600; border-bottom: 2px solid var(--border-color); text-transform: uppercase; font-size: 0.75rem; letter-spacing: 0.05em; }\n");
        sb.append("tr { border-bottom: 1px solid var(--border-color); transition: background-color 0.15s ease; }\n");
        sb.append("tr:last-child { border-bottom: none; }\n");
        sb.append("tr:hover { background-color: var(--hover-bg); }\n");
        sb.append(
                "a { color: var(--primary-color); text-decoration: none; font-weight: 500; display: inline-flex; align-items: center; }\n");
        sb.append("a:hover { text-decoration: underline; }\n");
        sb.append(".icon { margin-right: 0.5rem; font-size: 1rem; }\n");
        sb.append(".size, .date { color: var(--text-muted); text-align: right; white-space: nowrap; }\n");
        sb.append(".size { width: 15%; } .date { width: 25%; }\n");
        sb.append(
                ".footer { padding: 1rem 2rem; background-color: var(--hover-bg); border-top: 1px solid var(--border-color); font-size: 0.8rem; color: var(--text-muted); text-align: right; }\n");
        sb.append("</style>\r\n");
        sb.append("</head>\r\n");

        sb.append("<body>\r\n");
        sb.append("<div class=\"container\">\r\n");
        sb.append("<div class=\"header-bar\">\r\n");
        sb.append("<h1>");
        sb.append(sm.getString("defaultServlet.directory.title", escapedDirectoryWebappPath));

        // Render the link to our parent (if required)
        String parentDirectory = directoryWebappPath;
        if (parentDirectory.endsWith("/")) {
            parentDirectory = parentDirectory.substring(0, parentDirectory.length() - 1);
        }
        final int slash = parentDirectory.lastIndexOf('/');
        if (slash >= 0) {
            String parent = directoryWebappPath.substring(0, slash);
            sb.append(" \u2013 <a href=\"");
            sb.append(rewrittenContextPath);
            if (parent.isEmpty()) {
                parent = "/";
            }
            sb.append(rewriteUrl(parent));
            if (!parent.endsWith("/")) {
                sb.append('/');
            }
            sb.append("\">");
            sb.append("<span class=\"icon\">⬆️</span> ");
            sb.append(sm.getString("defaultServlet.directory.parent",
                    org.apache.tomcat.util.security.Escape.htmlElementContent(parent)));
            sb.append("</a>");
        }

        sb.append("</h1>\r\n");
        sb.append("</div>\r\n");

        sb.append("<table>\r\n");

        final Order order;
        if (sortListings) {
            order = sortManager.getOrder(request.getQueryString());
        } else {
            order = null;
        }

        // Render the column headings
        sb.append("<thead>\r\n");
        sb.append("<tr>\r\n");
        sb.append("<th>");
        if (order != null) {
            sb.append("<a href=\"?C=N;O=").append(invokeGetOrderChar(order, 'N')).append("\">");
            sb.append(sm.getString("defaultServlet.resource.name"));
            sb.append("</a>");
        } else {
            sb.append(sm.getString("defaultServlet.resource.name"));
        }
        sb.append("</th>\r\n");

        sb.append("<th class=\"size\">");
        if (order != null) {
            sb.append("<a href=\"?C=S;O=").append(invokeGetOrderChar(order, 'S')).append("\">");
            sb.append(sm.getString("defaultServlet.resource.size"));
            sb.append("</a>");
        } else {
            sb.append(sm.getString("defaultServlet.resource.size"));
        }
        sb.append("</th>\r\n");

        sb.append("<th class=\"date\">");
        if (order != null) {
            sb.append("<a href=\"?C=M;O=").append(invokeGetOrderChar(order, 'M')).append("\">");
            sb.append(sm.getString("defaultServlet.resource.lastModified"));
            sb.append("</a>");
        } else {
            sb.append(sm.getString("defaultServlet.resource.lastModified"));
        }
        sb.append("</th>\r\n");
        sb.append("</tr>\r\n");
        sb.append("</thead>\r\n");

        if (null != sortManager) {
            sortManager.sort(entries, request.getQueryString());
        }

        sb.append("<tbody>\r\n");
        for (final org.apache.catalina.WebResource childResource : entries) {
            final String filename = childResource.getName();
            if ("WEB-INF".equalsIgnoreCase(filename) || "META-INF".equalsIgnoreCase(filename)) {
                continue;
            }
            if (!childResource.exists()) {
                continue;
            }

            sb.append("<tr>\r\n");

            sb.append("<td>\r\n");
            sb.append("<a href=\"");
            sb.append(rewrittenContextPath);
            sb.append(rewriteUrl(childResource.getWebappPath()));
            if (childResource.isDirectory()) {
                sb.append('/');
            }
            sb.append("\">");

            // Add folder/file icons
            if (childResource.isDirectory()) {
                sb.append("<span class=\"icon\">📁</span>");
            } else {
                sb.append("<span class=\"icon\">📄</span>");
            }

            sb.append(org.apache.tomcat.util.security.Escape.htmlElementContent(filename));
            if (childResource.isDirectory()) {
                sb.append('/');
            }
            sb.append("</a></td>\r\n");

            sb.append("<td class=\"size\">");
            if (!childResource.isDirectory()) {
                sb.append(renderSize(childResource.getContentLength()));
            }
            sb.append("</td>\r\n");

            sb.append("<td class=\"date\">");
            sb.append(renderTimestamp(childResource.getLastModified()));
            sb.append("</td>\r\n");

            sb.append("</tr>\r\n");
        }
        sb.append("</tbody>\r\n");
        sb.append("</table>\r\n");

        final String readme = getReadme(resource, encoding);
        if (readme != null) {
            sb.append("<div style=\"padding: 2rem;\">\r\n");
            sb.append(readme);
            sb.append("</div>\r\n");
        }

        sb.append("<div class=\"footer\">\r\n");
        if (showServerInfo) {
            sb.append(org.apache.catalina.util.ServerInfo.getServerInfo()).append(" &bull; ");
            sb.append("Powered by Apache Tomcat\r\n");
        }
        sb.append("</div>\r\n");

        sb.append("</div>\r\n"); // End container
        sb.append("</body>\r\n");
        sb.append("</html>\r\n");

        // Return an input stream to the underlying bytes
        writer.write(sb.toString());
        writer.flush();
        return new java.io.ByteArrayInputStream(stream.toByteArray());
    }
}