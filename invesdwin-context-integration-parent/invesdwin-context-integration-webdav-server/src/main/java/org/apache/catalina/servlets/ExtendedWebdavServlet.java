package org.apache.catalina.servlets;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

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
}