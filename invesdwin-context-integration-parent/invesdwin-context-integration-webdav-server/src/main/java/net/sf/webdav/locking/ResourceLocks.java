package net.sf.webdav.locking;

import java.util.Enumeration;
import java.util.Hashtable;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.spi.ITransaction;

/**
 * simple locking management for concurrent data access, NOT the webdav locking. ( could that be used instead? )
 * 
 * IT IS ACTUALLY USED FOR DOLOCK
 * 
 * @author re
 */
@NotThreadSafe
public class ResourceLocks implements IResourceLocks {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ResourceLocks.class);

    protected int cleanupCounter = 0;

    /**
     * keys: path value: LockedObject from that path Concurrent access can occur
     */
    protected Hashtable<String, LockedObject> locks = new Hashtable<String, LockedObject>();

    /**
     * keys: id value: LockedObject from that id Concurrent access can occur
     */
    protected Hashtable<String, LockedObject> locksByID = new Hashtable<String, LockedObject>();

    /**
     * keys: path value: Temporary LockedObject from that path Concurrent access can occur
     */
    protected Hashtable<String, LockedObject> tempLocks = new Hashtable<String, LockedObject>();

    /**
     * keys: id value: Temporary LockedObject from that id Concurrent access can occur
     */
    protected Hashtable<String, LockedObject> tempLocksByID = new Hashtable<String, LockedObject>();

    // REMEMBER TO REMOVE UNUSED LOCKS FROM THE HASHTABLE AS WELL

    protected LockedObject root = null;

    protected LockedObject tempRoot = null;

    /**
     * after creating this much LockedObjects, a cleanup deletes unused LockedObjects
     */
    private final int cleanupLimit = 100000;

    private final boolean temporary = true;

    public ResourceLocks() {
        this.root = new LockedObject(this, "/", true);
        this.tempRoot = new LockedObject(this, "/", false);
    }

    @Override
    public synchronized boolean lock(final ITransaction transaction, final String path, final String owner,
            final boolean exclusive, final int depth, final int timeout, final boolean temporary)
            throws LockFailedException {

        LockedObject lo = null;

        if (temporary) {
            lo = generateTempLockedObjects(transaction, path);
            lo.type = "read";
        } else {
            lo = generateLockedObjects(transaction, path);
            lo.type = "write";
        }

        if (lo.checkLocks(exclusive, depth)) {

            lo.exclusive = exclusive;
            lo.lockDepth = depth;
            //CHECKSTYLE:OFF
            lo.expiresAt = System.currentTimeMillis() + (timeout * 1000);
            //CHECKSTYLE:ON
            if (lo.parent != null) {
                lo.parent.expiresAt = lo.expiresAt;
                if (lo.parent.equals(this.root)) {
                    final LockedObject rootLo = getLockedObjectByPath(transaction, this.root.getPath());
                    rootLo.expiresAt = lo.expiresAt;
                } else if (lo.parent.equals(this.tempRoot)) {
                    final LockedObject tempRootLo = getTempLockedObjectByPath(transaction, this.tempRoot.getPath());
                    tempRootLo.expiresAt = lo.expiresAt;
                }
            }
            if (lo.addLockedObjectOwner(owner)) {
                return true;
            } else {
                LOG.trace("Couldn't set owner \"" + owner + "\" to resource at '" + path + "'");
                return false;
            }
        } else {
            // can not lock
            LOG.trace("Lock resource at " + path + " failed because"
                    + "\na parent or child resource is currently locked");
            return false;
        }
    }

    @Override
    public synchronized boolean unlock(final ITransaction transaction, final String id, final String owner) {

        if (this.locksByID.containsKey(id)) {
            final String path = this.locksByID.get(id).getPath();
            if (this.locks.containsKey(path)) {
                final LockedObject lo = this.locks.get(path);
                lo.removeLockedObjectOwner(owner);

                if (lo.children == null && lo.owner == null) {
                    lo.removeLockedObject();
                }

            } else {
                // there is no lock at that path. someone tried to unlock it
                // anyway. could point to a problem
                LOG.trace("net.sf.webdav.locking.ResourceLocks.unlock(): no lock for path " + path);
                return false;
            }

            if (this.cleanupCounter > this.cleanupLimit) {
                this.cleanupCounter = 0;
                cleanLockedObjects(transaction, this.root, !this.temporary);
            }
        }
        checkTimeouts(transaction, !this.temporary);

        return true;

    }

    @Override
    public synchronized void unlockTemporaryLockedObjects(final ITransaction transaction, final String path,
            final String owner) {
        if (this.tempLocks.containsKey(path)) {
            final LockedObject lo = this.tempLocks.get(path);
            lo.removeLockedObjectOwner(owner);

        } else {
            // there is no lock at that path. someone tried to unlock it
            // anyway. could point to a problem
            LOG.trace("net.sf.webdav.locking.ResourceLocks.unlock(): no lock for path " + path);
        }

        if (this.cleanupCounter > this.cleanupLimit) {
            this.cleanupCounter = 0;
            cleanLockedObjects(transaction, this.tempRoot, this.temporary);
        }

        checkTimeouts(transaction, this.temporary);

    }

    @Override
    public void checkTimeouts(final ITransaction transaction, final boolean temporary) {
        if (!temporary) {
            final Enumeration<LockedObject> lockedObjects = this.locks.elements();
            while (lockedObjects.hasMoreElements()) {
                final LockedObject currentLockedObject = lockedObjects.nextElement();

                //CHECKSTYLE:OFF
                if (currentLockedObject.expiresAt < System.currentTimeMillis()) {
                    //CHECKSTYLE:ON
                    currentLockedObject.removeLockedObject();
                }
            }
        } else {
            final Enumeration<LockedObject> lockedObjects = this.tempLocks.elements();
            while (lockedObjects.hasMoreElements()) {
                final LockedObject currentLockedObject = lockedObjects.nextElement();

                //CHECKSTYLE:OFF
                if (currentLockedObject.expiresAt < System.currentTimeMillis()) {
                    //CHECKSTYLE:ON
                    currentLockedObject.removeTempLockedObject();
                }
            }
        }

    }

    @Override
    public boolean exclusiveLock(final ITransaction transaction, final String path, final String owner, final int depth,
            final int timeout) throws LockFailedException {
        return lock(transaction, path, owner, true, depth, timeout, false);
    }

    @Override
    public boolean sharedLock(final ITransaction transaction, final String path, final String owner, final int depth,
            final int timeout) throws LockFailedException {
        return lock(transaction, path, owner, false, depth, timeout, false);
    }

    @Override
    public LockedObject getLockedObjectByID(final ITransaction transaction, final String id) {
        if (this.locksByID.containsKey(id)) {
            return this.locksByID.get(id);
        } else {
            return null;
        }
    }

    @Override
    public LockedObject getLockedObjectByPath(final ITransaction transaction, final String path) {
        if (this.locks.containsKey(path)) {
            return this.locks.get(path);
        } else {
            return null;
        }
    }

    @Override
    public LockedObject getTempLockedObjectByID(final ITransaction transaction, final String id) {
        if (this.tempLocksByID.containsKey(id)) {
            return this.tempLocksByID.get(id);
        } else {
            return null;
        }
    }

    @Override
    public LockedObject getTempLockedObjectByPath(final ITransaction transaction, final String path) {
        if (this.tempLocks.containsKey(path)) {
            return this.tempLocks.get(path);
        } else {
            return null;
        }
    }

    /**
     * generates real LockedObjects for the resource at path and its parent folders. does not create new LockedObjects
     * if they already exist
     * 
     * @param transaction
     * @param path
     *            path to the (new) LockedObject
     * @return the LockedObject for path.
     */
    private LockedObject generateLockedObjects(final ITransaction transaction, final String path) {
        if (!this.locks.containsKey(path)) {
            final LockedObject returnObject = new LockedObject(this, path, !this.temporary);
            final String parentPath = getParentPath(path);
            if (parentPath != null) {
                final LockedObject parentLockedObject = generateLockedObjects(transaction, parentPath);
                parentLockedObject.addChild(returnObject);
                returnObject.parent = parentLockedObject;
            }
            return returnObject;
        } else {
            // there is already a LockedObject on the specified path
            return this.locks.get(path);
        }

    }

    /**
     * generates temporary LockedObjects for the resource at path and its parent folders. does not create new
     * LockedObjects if they already exist
     * 
     * @param transaction
     * @param path
     *            path to the (new) LockedObject
     * @return the LockedObject for path.
     */
    private LockedObject generateTempLockedObjects(final ITransaction transaction, final String path) {
        if (!this.tempLocks.containsKey(path)) {
            final LockedObject returnObject = new LockedObject(this, path, this.temporary);
            final String parentPath = getParentPath(path);
            if (parentPath != null) {
                final LockedObject parentLockedObject = generateTempLockedObjects(transaction, parentPath);
                parentLockedObject.addChild(returnObject);
                returnObject.parent = parentLockedObject;
            }
            return returnObject;
        } else {
            // there is already a LockedObject on the specified path
            return this.tempLocks.get(path);
        }

    }

    /**
     * deletes unused LockedObjects and resets the counter. works recursively starting at the given LockedObject
     * 
     * @param transaction
     * @param lo
     *            LockedObject
     * @param temporary
     *            Clean temporary or real locks
     * 
     * @return if cleaned
     */
    private boolean cleanLockedObjects(final ITransaction transaction, final LockedObject lo, final boolean temporary) {

        if (lo.children == null) {
            if (lo.owner == null) {
                if (temporary) {
                    lo.removeTempLockedObject();
                } else {
                    lo.removeLockedObject();
                }

                return true;
            } else {
                return false;
            }
        } else {
            boolean canDelete = true;
            int limit = lo.children.length;
            for (int i = 0; i < limit; i++) {
                if (!cleanLockedObjects(transaction, lo.children[i], temporary)) {
                    canDelete = false;
                } else {

                    // because the deleting shifts the array
                    i--;
                    limit--;
                }
            }
            if (canDelete) {
                if (lo.owner == null) {
                    if (temporary) {
                        lo.removeTempLockedObject();
                    } else {
                        lo.removeLockedObject();
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    /**
     * creates the parent path from the given path by removing the last '/' and everything after that
     * 
     * @param path
     *            the path
     * @return parent path
     */
    private String getParentPath(final String path) {
        final int slash = path.lastIndexOf('/');
        if (slash == -1) {
            return null;
        } else {
            if (slash == 0) {
                // return "root" if parent path is empty string
                return "/";
            } else {
                return path.substring(0, slash);
            }
        }
    }

}
