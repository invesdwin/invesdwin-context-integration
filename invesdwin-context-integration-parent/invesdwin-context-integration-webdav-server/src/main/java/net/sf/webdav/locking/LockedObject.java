package net.sf.webdav.locking;

import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * a helper class for ResourceLocks, represents the Locks
 * 
 * @author re
 * 
 */
@NotThreadSafe
public class LockedObject {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(LockedObject.class);

    /**
     * Describing the depth of a locked collection. If the locked resource is not a collection, depth is 0 / doesn't
     * matter.
     */
    protected int lockDepth;

    /**
     * Describing the timeout of a locked object (ms)
     */
    protected long expiresAt;

    /**
     * owner of the lock. shared locks can have multiple owners. is null if no owner is present
     */
    protected String[] owner = null;

    /**
     * children of that lock
     */
    protected LockedObject[] children = null;

    protected LockedObject parent = null;

    /**
     * weather the lock is exclusive or not. if owner=null the exclusive value doesn't matter
     */
    protected boolean exclusive = false;

    /**
     * weather the lock is a write or read lock
     */
    protected String type = null;

    private final ResourceLocks resourceLocks;

    private final String path;

    private final String id;

    /**
     * @param resLocks
     *            the resourceLocks where locks are stored
     * @param path
     *            the path to the locked object
     * @param temporary
     *            indicates if the LockedObject should be temporary or not
     */
    public LockedObject(final ResourceLocks resLocks, final String path, final boolean temporary) {
        this.path = path;
        this.id = UUID.randomUUID().toString();
        this.resourceLocks = resLocks;

        if (!temporary) {
            this.resourceLocks.locks.put(path, this);
            this.resourceLocks.locksByID.put(this.id, this);
        } else {
            this.resourceLocks.tempLocks.put(path, this);
            this.resourceLocks.tempLocksByID.put(this.id, this);
        }
        this.resourceLocks.cleanupCounter++;
    }

    /**
     * adds a new owner to a lock
     * 
     * @param owner
     *            string that represents the owner
     * @return true if the owner was added, false otherwise
     */
    public boolean addLockedObjectOwner(final String owner) {

        if (this.owner == null) {
            this.owner = new String[1];
        } else {

            final int size = this.owner.length;
            final String[] newLockObjectOwner = new String[size + 1];

            // check if the owner is already here (that should actually not
            // happen)
            for (int i = 0; i < size; i++) {
                if (this.owner[i].equals(owner)) {
                    return false;
                }
            }

            System.arraycopy(this.owner, 0, newLockObjectOwner, 0, size);
            this.owner = newLockObjectOwner;
        }

        this.owner[this.owner.length - 1] = owner;
        return true;
    }

    /**
     * tries to remove the owner from the lock
     * 
     * @param owner
     *            string that represents the owner
     */
    public void removeLockedObjectOwner(final String owner) {

        try {
            if (this.owner != null) {
                final int size = this.owner.length;
                for (int i = 0; i < size; i++) {
                    // check every owner if it is the requested one
                    if (this.owner[i].equals(owner)) {
                        // remove the owner
                        final String[] newLockedObjectOwner = new String[size - 1];
                        for (int j = 0; j < (size - 1); j++) {
                            if (j < i) {
                                newLockedObjectOwner[j] = this.owner[j];
                            } else {
                                newLockedObjectOwner[j] = this.owner[j + 1];
                            }
                        }
                        this.owner = newLockedObjectOwner;

                    }
                }
                if (this.owner.length == 0) {
                    this.owner = null;
                }
            }
        } catch (final ArrayIndexOutOfBoundsException e) {
            LOG.warn("LockedObject.removeLockedObjectOwner(): %s", e);
        }
    }

    /**
     * adds a new child lock to this lock
     * 
     * @param newChild
     *            new child
     */
    public void addChild(final LockedObject newChild) {
        if (this.children == null) {
            this.children = new LockedObject[0];
        }
        final int size = this.children.length;
        final LockedObject[] newChildren = new LockedObject[size + 1];
        System.arraycopy(this.children, 0, newChildren, 0, size);
        newChildren[size] = newChild;
        this.children = newChildren;
    }

    /**
     * deletes this Lock object. assumes that it has no children and no owners (does not check this itself)
     * 
     */
    public void removeLockedObject() {
        if (this != this.resourceLocks.root && !this.getPath().equals("/")) {

            final int size = this.parent.children.length;
            for (int i = 0; i < size; i++) {
                if (this.parent.children[i].equals(this)) {
                    final LockedObject[] newChildren = new LockedObject[size - 1];
                    for (int i2 = 0; i2 < (size - 1); i2++) {
                        if (i2 < i) {
                            newChildren[i2] = this.parent.children[i2];
                        } else {
                            newChildren[i2] = this.parent.children[i2 + 1];
                        }
                    }
                    if (newChildren.length != 0) {
                        this.parent.children = newChildren;
                    } else {
                        this.parent.children = null;
                    }
                    break;
                }
            }

            // removing from hashtable
            this.resourceLocks.locksByID.remove(getID());
            this.resourceLocks.locks.remove(getPath());

            // now the garbage collector has some work to do
        }
    }

    /**
     * deletes this Lock object. assumes that it has no children and no owners (does not check this itself)
     * 
     */
    public void removeTempLockedObject() {
        if (this != this.resourceLocks.tempRoot) {
            // removing from tree
            if (this.parent != null && this.parent.children != null) {
                final int size = this.parent.children.length;
                for (int i = 0; i < size; i++) {
                    if (this.parent.children[i].equals(this)) {
                        final LockedObject[] newChildren = new LockedObject[size - 1];
                        for (int i2 = 0; i2 < (size - 1); i2++) {
                            if (i2 < i) {
                                newChildren[i2] = this.parent.children[i2];
                            } else {
                                newChildren[i2] = this.parent.children[i2 + 1];
                            }
                        }
                        if (newChildren.length != 0) {
                            this.parent.children = newChildren;
                        } else {
                            this.parent.children = null;
                        }
                        break;
                    }
                }

                // removing from hashtable
                this.resourceLocks.tempLocksByID.remove(getID());
                this.resourceLocks.tempLocks.remove(getPath());

                // now the garbage collector has some work to do
            }
        }
    }

    /**
     * checks if a lock of the given exclusivity can be placed, only considering children up to "depth"
     * 
     * @param exclusive
     *            wheather the new lock should be exclusive
     * @param depth
     *            the depth to which should be checked
     * @return true if the lock can be placed
     */
    public boolean checkLocks(final boolean exclusive, final int depth) {
        return checkParents(exclusive) && checkChildren(exclusive, depth);
    }

    /**
     * helper of checkLocks(). looks if the parents are locked
     * 
     * @param exclusive
     *            wheather the new lock should be exclusive
     * @return true if no locks at the parent path are forbidding a new lock
     */
    private boolean checkParents(final boolean exclusive) {
        if ("/".equals(this.path)) {
            return true;
        } else {
            if (this.owner == null) {
                // no owner, checking parents
                return this.parent != null && this.parent.checkParents(exclusive);
            } else {
                // there already is a owner
                return !(this.exclusive || exclusive) && this.parent.checkParents(exclusive);
            }
        }
    }

    /**
     * helper of checkLocks(). looks if the children are locked
     * 
     * @param exclusive
     *            wheather the new lock should be exclusive
     * @return true if no locks at the children paths are forbidding a new lock
     * @param depth
     *            depth
     */
    private boolean checkChildren(final boolean exclusive, final int depth) {
        if (this.children == null) {
            // a file

            return this.owner == null || !(this.exclusive || exclusive);
        } else {
            // a folder

            if (this.owner == null) {
                // no owner, checking children

                if (depth != 0) {
                    boolean canLock = true;
                    final int limit = this.children.length;
                    for (int i = 0; i < limit; i++) {
                        if (!this.children[i].checkChildren(exclusive, depth - 1)) {
                            canLock = false;
                        }
                    }
                    return canLock;
                } else {
                    // depth == 0 -> we don't care for children
                    return true;
                }
            } else {
                // there already is a owner
                return !(this.exclusive || exclusive);
            }
        }

    }

    /**
     * Sets a new timeout for the LockedObject
     * 
     * @param timeout
     */
    public void refreshTimeout(final int timeout) {
        //CHECKSTYLE:OFF
        this.expiresAt = System.currentTimeMillis() + (timeout * 1000);
        //CHECKSTYLE:ON
    }

    /**
     * Gets the timeout for the LockedObject
     * 
     * @return timeout
     */
    public long getTimeoutMillis() {
        //CHECKSTYLE:OFF
        return (this.expiresAt - System.currentTimeMillis());
        //CHECKSTYLE:ON
    }

    /**
     * Return true if the lock has expired.
     * 
     * @return true if timeout has passed
     */
    public boolean hasExpired() {
        if (this.expiresAt != 0) {
            //CHECKSTYLE:OFF
            return (System.currentTimeMillis() > this.expiresAt);
            //CHECKSTYLE:ON
        } else {
            return true;
        }
    }

    /**
     * Gets the LockID (locktoken) for the LockedObject
     * 
     * @return locktoken
     */
    public String getID() {
        return this.id;
    }

    /**
     * Gets the owners for the LockedObject
     * 
     * @return owners
     */
    public String[] getOwner() {
        return this.owner;
    }

    /**
     * Gets the path for the LockedObject
     * 
     * @return path
     */
    public String getPath() {
        return this.path;
    }

    /**
     * Sets the exclusivity for the LockedObject
     * 
     * @param exclusive
     */
    public void setExclusive(final boolean exclusive) {
        this.exclusive = exclusive;
    }

    /**
     * Gets the exclusivity for the LockedObject
     * 
     * @return exclusivity
     */
    public boolean isExclusive() {
        return this.exclusive;
    }

    /**
     * Gets the exclusivity for the LockedObject
     * 
     * @return exclusivity
     */
    public boolean isShared() {
        return !this.exclusive;
    }

    /**
     * Gets the type of the lock
     * 
     * @return type
     */
    public String getType() {
        return this.type;
    }

    /**
     * Gets the depth of the lock
     * 
     * @return depth
     */
    public int getLockDepth() {
        return this.lockDepth;
    }

}
