/*
 * DocNodeContext.java
 */
package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.DocException;
import org.docma.coreapi.DocGroup;
import org.docma.coreapi.DocNode;
import org.docma.coreapi.dbimplementation.dblayer.DbNode;
import org.hibernate.Session;
/**
 *
 * @author MP
 */
public interface DocNodeContext 
{
    Session getDbSession();
    long getDbSessionCounter();
    long getDbTransactionCounter();
    void activateNode(DbNode nd);
    DocStoreDbImpl getDocStore();
    DocNodeDbImpl createDocNodeFromDbNode(DbNode db_node);
    void refreshAliasList();

    //
    // AbstractDocStoreSession methods that are used by the nodes:
    //
    String getTranslationMode();
    String getUserId();
    void nodeAddedEvent(DocGroup parent, DocNode node);
    void nodeRemovedEvent(DocGroup parent, DocNode node);
    void nodeChangedEvent(DocGroup parent, DocNode node, String lang);
    boolean startLocalTransaction();
    boolean startLocalTransaction(boolean clear_cache);
    boolean runningTransaction();
    void commitLocalTransaction(boolean started);
    void rollbackLocalTransaction(boolean started);
    void rollbackLocalTransactionRuntime(boolean started, Exception ex);
    void rollbackLocalTransactionRethrow(boolean started, Exception ex) throws DocException;
    
}
