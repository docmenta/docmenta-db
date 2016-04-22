/*
 * LockManagerDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.lockapi.*;
import org.docma.hibernate.DbWork;
import org.docma.hibernate.HibernateUtil;
import org.docma.hibernate.DbConnectionData;

// import org.hibernate.*;

/**
 *
 * @author MP
 */
public class LockManagerDbImpl extends AbstractLockManager implements LockManager
{
    private DbWork db;
    private String storeId;
    private DocVersionId versionId;
    private VersionIdFactory versionIdFactory;

    private int versionDbId;

    public LockManagerDbImpl(String storeId, 
                             DocVersionId verId,
                             VersionIdFactory verIdFactory,
                             DbConnectionData con_data)
    {
        this(storeId, verId, verIdFactory, HibernateUtil.getSessionFactory(con_data));
    }

    LockManagerDbImpl(String storeId, 
                      DocVersionId verId,
                      VersionIdFactory verIdFactory,
                      org.hibernate.SessionFactory dbFactory)
    {
        this.db = new DbWork(dbFactory);
        this.storeId = storeId;
        this.versionId = verId;
        this.versionIdFactory = verIdFactory;

        try {
            db.startRead();
            DbStore store = DbUtil.getDbStore(db.getSession(), storeId);
            DbVersion ver = DbUtil.getDbVersion(store, versionId, versionIdFactory);
            this.versionDbId = ver.getVersionDbId();
            db.commit();
        } catch (Exception ex) {
            db.rollbackRuntime(ex);
        }
    }

    /* --------------  Interface LockManager  --------------- */

    public synchronized boolean setLock(String objId, String lockname, String user, long timeout)
    {
        LockImpl existinglock = null;
        LockImpl newlock = null;
        boolean timed_out = false;
        try {
            db.startUpdate();
            DbNode db_node = getDbNode(objId);
            if (db_node == null) {
                throw new DocRuntimeException("Could not set lock. Node does not exist: " + objId);
            }
            DbNodeLock db_lock = db_node.getLock(lockname);
            long currentTime = System.currentTimeMillis();
            if (db_lock != null) {
                existinglock = new LockImpl(objId, lockname,
                                            db_lock.getUserId(),
                                            db_lock.getCreationTime(),
                                            db_lock.getTimeout());
                if (checkLockTimeout(currentTime, existinglock)) {
                    timed_out = true;
                    db_node.removeLock(lockname);
                    db_lock = null;
                }
            }
            if (db_lock == null) {
                newlock = new LockImpl(objId, lockname, user, currentTime, timeout);
                db_node.addLock(lockname, user, currentTime, timeout);
            }
            db.commit();
        } catch (Exception ex) {
            db.rollbackSilent(ex);
            ex.printStackTrace();
            existinglock = null;
            newlock = null;
        }
        if (timed_out && (existinglock != null)) lockTimeoutEvent(existinglock);
        if (newlock != null) {
            lockAddedEvent(newlock);
            return true;
        } else {
            return false;
        }
    }

    public synchronized boolean refreshLock(String objId, String lockname, long timeout)
    {
        boolean refreshed = false;
        try {
            db.startUpdate();
            DbNode db_node = getDbNode(objId);
            if (db_node == null) {
                throw new DocRuntimeException("Could not refresh lock. Node does not exist: " + objId);
            }
            DbNodeLock db_lock = db_node.getLock(lockname);
            if (db_lock != null) {
                long currentTime = System.currentTimeMillis();
                long newtimeout = timeout + (currentTime - db_lock.getCreationTime());
                db_lock.setTimeout(newtimeout);
                refreshed = true;
            }
            db.commit();
        } catch (Exception ex) {
            db.rollbackSilent(ex);
            ex.printStackTrace();
            refreshed = false;
        }
        return refreshed;
    }

    // public Lock[] getLocks(String objId) {
    //     return null;
    // }

    public synchronized Lock getLock(String objId, String lockname)
    {
        LockImpl lock = null;
        boolean timed_out = false;
        try {
            db.startUpdate();
            DbNode db_node = getDbNode(objId);
            DbNodeLock db_lock = db_node.getLock(lockname);
            if (db_lock != null) {
                lock = new LockImpl(objId, lockname,
                                    db_lock.getUserId(),
                                    db_lock.getCreationTime(),
                                    db_lock.getTimeout());
                long currentTime = System.currentTimeMillis();
                if (checkLockTimeout(currentTime, lock)) {
                    timed_out = true;
                    db_node.removeLock(lockname);
                }
            }
            db.commit();
        } catch (Exception ex) {
            db.rollbackSilent(ex);
            ex.printStackTrace();
            lock = null;
        }
        if (timed_out && (lock != null)) {
            lockTimeoutEvent(lock);
            return null;
        } else {
            return lock;
        }
    }

    // public Lock[] removeLocks(String objId) {
    //     return null;
    // }

    public synchronized Lock removeLock(String objId, String lockname)
    {
        LockImpl lock = null;
        try {
            db.startUpdate();
            DbNode db_node = getDbNode(objId);
            DbNodeLock db_lock = db_node.getLock(lockname);
            if (db_lock != null) {
                lock = new LockImpl(objId, lockname,
                                    db_lock.getUserId(),
                                    db_lock.getCreationTime(),
                                    db_lock.getTimeout());
                db_node.removeLock(lockname);
            }
            db.commit();
        } catch (Exception ex) {
            db.rollbackSilent(ex);
            ex.printStackTrace();
            lock = null;
        }
        if (lock != null) lockRemovedEvent(lock);
        return lock;
    }

    /* --------------  Private methods  --------------- */


    private DbNode getDbNode(String nodeNum)
    {
        return DbUtil.getDbNode(db.getSession(), getVersionDbId(), nodeNum);
    }

    private int getVersionDbId()
    {
        return versionDbId;
    }
}
