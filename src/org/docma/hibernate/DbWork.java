/*
 * DbWork.java
 */

package org.docma.hibernate;

import org.docma.coreapi.*;
import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DbWork
{
    private final int MODE_READ = 0;
    private final int MODE_UPDATE = 1;
    private long SESS_TIMEOUT = 1000*60*60*8;  // reopen hibernate session after 8 hours by default

    private org.hibernate.SessionFactory factory = null;
    private org.hibernate.Session sess = null;
    private org.hibernate.Transaction tx = null;

    private int currentWorkMode = -1;
    private long sessOpenedTime;

    public DbWork(org.hibernate.SessionFactory factory)
    {
        this.factory = factory;
    }

    public DbWork(org.hibernate.SessionFactory factory, long readCacheTimeout)
    {
        this.factory = factory;
        this.SESS_TIMEOUT = readCacheTimeout;
    }

    public Session startRead()
    {
        openWork(MODE_READ);
        return sess;
    }

    public Session startUpdate()
    {
        openWork(MODE_UPDATE);
        return sess;
    }

    private void openWork(int mode)
    {
        long now = System.currentTimeMillis();
        boolean reopen = (mode != currentWorkMode) ||
                         ((now - sessOpenedTime) >= SESS_TIMEOUT);
        if (reopen && (sess != null)) {
            try { sess.close(); } catch (Throwable th) {}
            sess = null;
        }
        if (reopen || (sess == null) || !sess.isOpen()) {
            sess = factory.openSession();
            sess.setDefaultReadOnly(mode == MODE_READ);
            currentWorkMode = mode;
            sessOpenedTime = now;
        }
        tx = sess.beginTransaction();
    }

    public void commit()
    {
        tx.commit();
        tx = null;
        closeWork();
    }

    public void rollbackSilent(Exception ex)
    {
        if (tx != null) {
            tx.rollback();
            tx = null;
        }
        closeWork();
    }

    public void rollback(Exception ex) throws DocException
    {
        rollbackSilent(ex);
        if (ex instanceof DocException) throw (DocException) ex;
        else throw new DocException(ex);
    }

    public void rollbackRuntime(Exception ex)
    {
        rollbackSilent(ex);
        if (ex instanceof DocRuntimeException) throw (DocRuntimeException) ex;
        else throw new DocRuntimeException(ex);
    }
    
    public void close()
    {
        if (sess != null) sess.close();
    }

    private void closeWork()
    {
        // If MODE_UPDATE, then close session (i.e. do not keep objects in cache).
        // If MODE_READ and session timeout is positive, then do nothing here,
        // i.e. session is kept open until timeout occurs!
        // This improves performance, if many successive read operations are
        // executed (objects are kept in cache, i.e. do not have to be read from
        // database on every call of a getXxxx()).
        if ((currentWorkMode == MODE_UPDATE) || (SESS_TIMEOUT <= 0)) {
            if (sess != null) sess.close();
            sess = null;
        }
    }

    public Session getSession()
    {
        return sess;
    }
}
