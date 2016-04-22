/*
 * DocStoreDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.lockapi.LockManager;
import org.docma.hibernate.DbConnectionData;

/**
 *
 * @author MP
 */
public class DocStoreDbImpl extends AbstractDocStore
{

    public DocStoreDbImpl(String storeId, 
                          DocVersionId verId, 
                          VersionIdFactory verIdFact, 
                          DbConnectionData conData)
    {
        super(storeId, verId);
        LockManager lm = new LockManagerDbImpl(storeId, verId, verIdFact, conData);
        setLockManager(lm);
    }
    
    DocStoreDbImpl(String storeId, DocVersionId verId, LockManager lm)
    {
        super(storeId, verId);
        setLockManager(lm);
    }


}
