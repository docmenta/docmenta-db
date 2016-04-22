/*
 * DocStoreManagerDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.DocStoreSession;
import org.docma.coreapi.DocVersionId;
import org.docma.coreapi.implementation.*;
import org.docma.lockapi.LockManager;

/**
 *
 * @author MP
 */
public class DocStoreManagerDbImpl extends AbstractDocStoreManager
{
    private org.hibernate.SessionFactory factory;

    public DocStoreManagerDbImpl(org.hibernate.SessionFactory factory)
    {
        this.factory = factory;
    }

    protected DocStoreSession createSessionInstance(String sessionId, String userId)
    {
        throw new RuntimeException("Method not implemented.");
        // To do: implement class DocStoreSessionDbImpl.
        // return new DocStoreSessionDbImpl(factory, this, sessionId, userId);
    }

    protected AbstractDocStore createStoreInstance(DocStoreSession sess, String storeId, DocVersionId verId)
    {
        LockManager lm = new LockManagerDbImpl(storeId, verId, getVersionIdFactory(), factory);
        return new DocStoreDbImpl(storeId, verId, lm);
    }

}
