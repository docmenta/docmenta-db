/*
 * DocNodeDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import java.util.*;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;
import org.docma.util.*;

import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DocNodeDbImpl implements DocNode
{
    private final DocNodeContext docSession;
    private DbNode dbNode;
    private long dbNodeSessCounter;
    private boolean refresh_flag = false;
    private DocStoreDbImpl docStore;

    private String nodeId = null;
    private DocGroupDbImpl parentGroup = null;


    DocNodeDbImpl(DocNodeContext docSession, DbNode dbNode)
    {
        this.docSession = docSession;
        this.dbNode = dbNode;
        this.dbNodeSessCounter = docSession.getDbSessionCounter();
        this.docStore = docSession.getDocStore();
        
        // Initialize nodeId for performance reasons.
        // Note: Constructor should be called inside a (local) transaction 
        // (in case dbNode is a proxy instance and has to be loaded from database). 
        this.nodeId = DbUtil.formatNodeNumber(dbNode.getNodeNumber());
    }

    /* --------------  Methods used in sub-classes  ---------------- */

    DocNodeContext getDocContext()
    {
        return docSession;
    }

    DocStoreDbImpl getDocStore()
    {
        return docStore;
    }

    org.hibernate.Session getDbSession()
    {
        return docSession.getDbSession();
    }

    /**
     * Get database node for read/update of basic properties
     * (dbNodeId, nodeNumber, child nodes, parent node, node type).
     * Attributes and contents can only be read and updated after node
     * is activated (i.e. call activateDbNode() before).
     */
    DbNode getDbNode()
    {
        if (DocConstants.DEBUG) {
            Log.info("Node session counter of node " + dbNode.getNodeDbId() + ": " + dbNodeSessCounter + 
                     ". Session counter: " + docSession.getDbSessionCounter());
            if (! docSession.runningTransaction()) {
                Log.error("Calling getDnNode() outside of transaction!");
            }
        }
        if (dbNodeSessCounter != docSession.getDbSessionCounter()) {
            if (dbNode.persisted()) {
                // dbNode has to be reconnected to new session
                Session db_sess = docSession.getDbSession();
                dbNode = (DbNode) db_sess.load(DbNode.class, dbNode.getNodeDbId());
            }
            dbNodeSessCounter = docSession.getDbSessionCounter();
        } else {
            if (refresh_flag) {
                Session db_sess = docSession.getDbSession();
                if (db_sess != null) db_sess.refresh(dbNode);
            }
        }
        refresh_flag = false;
        if (DocConstants.DEBUG) {
            if (docSession.getDbSession() == null) {
                Log.warning("getDbNode() is called outside of session context.");
            }
        }
        return dbNode;
    }

    /**
     * Get database node for read-only operations on attributes or binary/text
     * contents. The actual storage location of attributes/contents may be
     * in the last modified node of a previous version. Returns the database
     * node that atually contains the attributes/contents.
     */
    DbNode getDbNodeReal()
    {
        DbNode nd = getDbNode();
        long lastmodId = nd.getLastModNodeDbId();
        if ((lastmodId > 0) && (lastmodId != nd.getNodeDbId())) {
            Session db_sess = docSession.getDbSession();
            nd = (DbNode) db_sess.load(DbNode.class, lastmodId);
        }
        return nd;
    }

    /**
     * Activates database node. Returns activated database node.
     */
    DbNode activateDbNode()
    {
        DbNode nd = getDbNode();
        docSession.activateNode(nd);
        return nd;
    }
    
    void prepareUpdate(DbNodeEntity nd)
    {
        if (nd.persisted()) {   // if node is not transient
            getDbSession().setReadOnly(nd, false); // allow update
        }
    }
    
    void prepareUpdate(List nodeList)
    {
        if (nodeList == null) {
            return;
        }
        for (Object obj : nodeList) {
            prepareUpdate((DbNodeEntity) obj);
        }
    }

    void fireChangedEvent()
    {
        fireChangedEvent(docSession.getTranslationMode());
    }

    void fireChangedEvent(String lang)
    {
        DocGroup par = getParentGroup();
        if (par != null) {
            docSession.nodeChangedEvent(par, this, lang);
        }
    }

    boolean startLocalTransaction()
    {
        return docSession.startLocalTransaction();
    }

    boolean startLocalTransaction(boolean clear_cache)
    {
        return docSession.startLocalTransaction(clear_cache);
    }

    void commitLocalTransaction(boolean started)
    {
        docSession.commitLocalTransaction(started);
    }

    void rollbackLocalTransaction(boolean started)
    {
        docSession.rollbackLocalTransaction(started);
    }

    void rollbackLocalTransactionRuntime(boolean started, Exception ex)
    {
        docSession.rollbackLocalTransactionRuntime(started, ex);
    }

    void rollbackLocalTransactionRethrow(boolean started, Exception ex) throws DocException
    {
        docSession.rollbackLocalTransactionRethrow(started, ex);
    }

    protected Set getAttLanguageCodes()
    {
        Set res = new HashSet();
        DbNode nd = getDbNodeReal();
        Iterator it = nd.getAttributes().keySet().iterator();
        while (it.hasNext()) {
            String lang = ((NodeAttributeKey) it.next()).getLangCode();
            if (! lang.equals(LANG_ORIG)) res.add(lang);
        }
        return res;
    }

    /* --------------  Package local  ---------------- */

    long getDbId()
    {
        return getDbNode().getNodeDbId();
    }

    void setParentGroup(DocGroupDbImpl parentGroup)
    {
        this.parentGroup = parentGroup;
    }

    /* --------------  Public  ---------------------- */

    public boolean equals(Object obj)
    {
        if ((obj != null) && (obj instanceof DocNodeDbImpl)) {
          // synchronized (docSession) {
          //     boolean started = startLocalTransaction();
                 if (DocConstants.DEBUG) { 
                     Log.info("Call of DocNodeDbImpl.equals(): " + getDocContext().getDbTransactionCounter());
                 }
          //     try {
                    DocNodeDbImpl other = (DocNodeDbImpl) obj;
                    boolean res = (getDocStore() == other.getDocStore()) && getId().equals(other.getId());
          //         commitLocalTransaction(started);
                    return res;
          //     } catch (Exception ex) {
          //         rollbackLocalTransactionRuntime(started, ex);
          //         return false;  // is never reached
          //     }
          // }
        } else {
            return false;
        }
    }


    /* --------------  Interface DocNode ---------------------- */

    public void refresh()
    {
        if (DocConstants.DEBUG) {
            Log.info("Refreshing node " + dbNode.getNodeDbId());
        }
        synchronized (docSession) {
            long currentSessCounter = docSession.getDbSessionCounter();
            if (currentSessCounter == dbNodeSessCounter) {
                // dbNode has to be reconnected to new session
                // Session db_sess = docSession.getDbSession();
                // if (db_sess != null) {
                    // db_sess.refresh(dbNode);
                    refresh_flag = true;   // see getDbNode()
                // } else {
                //    // No session exists, therefore no refresh is needed, 
                //    // because dbNode will be loaded in methode getDbNode().
                // }
            } else {
                // New session was started, therefore no refresh needed, because
                // dbNode will be refreshed in methode getDbNode().
            }
        }
    }

    public String getId()
    {
        if (nodeId == null) {
            synchronized (docSession) {
                boolean started = startLocalTransaction();
                if (DocConstants.DEBUG) { 
                    Log.info("Local transaction for getId: " + started + ", " + getDocContext().getDbTransactionCounter());
                }
                try {
                    String res = DbUtil.formatNodeNumber(getDbNode().getNodeNumber());
                    commitLocalTransaction(started);
                    nodeId = res;
                } catch (Exception ex) {
                    rollbackLocalTransactionRuntime(started, ex);
                    return null;  // is never reached
                }
            }
        }
        return nodeId;
    }

    public String getTitle()
    {
        return getAttribute(DocAttributes.TITLE);
    }

    public String getTitle(String lang_id)
    {
        return getAttribute(DocAttributes.TITLE, lang_id);
    }

    public void setTitle(String title)
    {
        setAttribute(DocAttributes.TITLE, title);
    }

    public void setTitle(String title, String lang_id)
    {
        setAttribute(DocAttributes.TITLE, title, lang_id);
    }

    public String getAlias()
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            if (DocConstants.DEBUG) { 
                Log.info("Local transaction for getAlias: " + started + ", " + getDocContext().getDbTransactionCounter());
            }
            try {
                DbNode nd = getDbNode();
                String res = (nd.aliasCount() > 0) ? nd.getAlias(0) : null;
                commitLocalTransaction(started);
                return res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    public String[] getAliases()
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            if (DocConstants.DEBUG) { 
                Log.info("Local transaction for getAliases: " + started + ", " + getDocContext().getDbTransactionCounter());
            }
            try {
                String[] res = getDbNode().getAliases();
                commitLocalTransaction(started);
                return res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    /**
     * Insert as first alias to the list.
     * @param alias
     */
    public void addAlias(String alias)
    {
        if (alias.length() == 0) {
            throw new DocRuntimeException("Invalid alias: empty string.");
        }
        boolean added = false;
        synchronized (docSession) {
            boolean started = startLocalTransaction(true);
            try {
                DbNode nd = getDbNode();
                if (nd.aliasIndex(alias) < 0) {
                    prepareUpdate(nd);  // allow update
                    nd.insertAlias(0, alias);
                    added = true;
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        if (added) {
            fireChangedEvent();
            docSession.refreshAliasList();
        }
    }

    public void setAliases(String[] aliases) 
    {
        for (int i=0; i < aliases.length; i++) {
            if (aliases[i].length() == 0) {
                throw new DocRuntimeException("Invalid alias: empty string.");
            }
        }
        boolean added = false;
        synchronized (docSession) {
            boolean started = startLocalTransaction(true);
            try {
                DbNode nd = getDbNode();
                prepareUpdate(nd);  // allow update
                nd.setAliases(aliases);
                added = true;
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        if (added) {
            fireChangedEvent();
            docSession.refreshAliasList();
        }
    }

    public boolean deleteAlias(String alias)
    {
        boolean deleted = false;
        synchronized (docSession) {
            boolean started = startLocalTransaction(true);
            try {
                DbNode nd = getDbNode();
                prepareUpdate(nd);  // allow update
                deleted = nd.deleteAlias(alias);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        if (deleted) {
            fireChangedEvent();
            docSession.refreshAliasList();
        }
        return deleted;
    }

    public boolean hasAlias(String alias)
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            if (DocConstants.DEBUG) { 
                Log.info("Local transaction for hasAlias: " + started + ", " + getDocContext().getDbTransactionCounter());
            }
            try {
                boolean res = (getDbNode().aliasIndex(alias) >= 0);
                commitLocalTransaction(started);
                return res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return false;  // is never reached
            }
        }
    }

    /**
     * This is an utility method which is a combination of getAttributeNames() 
     * and getAttribute(name) to retrieve all attributes in one call. 
     * This method is more efficient than reading the attributes one by one, 
     * as this method reads all attributes in one single implicit transaction.
     * 
     * Note that the attribute values are as returned by method getAttribute(),
     * i.e. the value depends on the current translation mode.
     * 
     * @return Map of attribute name and value pairs.
     */
    public Map<String, String> getAttributes()
    {
        synchronized (docSession) {
            String lang_id = docSession.getTranslationMode();
            boolean started = startLocalTransaction();
            try {
                Map<String, String> result_map = new HashMap<String, String>();
                DbNode db_node = getDbNodeReal();
                Iterator it = db_node.getAttributes().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    NodeAttributeKey key = (NodeAttributeKey) entry.getKey();
                    if (key.getLangCode().equals(LANG_ORIG)) {
                        String attname = key.getAttName();
                        if (! DocAttributes.isInternalAttributeName(attname)) {
                            String val = (lang_id == null) ? (String) entry.getValue() 
                                                           : db_node.getAttribute(lang_id, attname);
                            if ((val == null) && (lang_id != null)) {  // if no translated value exists in translation mode
                                // return value of original language
                                val = (String) entry.getValue();
                            }
                            if (val == null) {
                                val = "";   // return empty string as default value
                            }
                            result_map.put(attname, val);
                        }
                    }
                }
                commitLocalTransaction(started);
                return result_map;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    /**
     * This is an utility method which is a combination of getAttributeNames() 
     * and getAttribute(name, lang_id) to retrieve all attributes in one call. 
     * This method is more efficient than reading the attributes one by one, 
     * as this method reads all attributes in one single implicit transaction.
     * 
     * @param lang_id The language id or null for the original language.
     * @return Map of attribute name and value pairs for the given language.
     */
    public Map<String, String> getAttributes(String lang_id)
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                String lang_key = (lang_id == null) ? LANG_ORIG : lang_id;  // transform null to string for database layer
                Map<String, String> result_map = new HashMap<String, String>();
                DbNode db_node = getDbNodeReal();
                Iterator it = db_node.getAttributes().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    NodeAttributeKey key = (NodeAttributeKey) entry.getKey();
                    if (lang_key.equals(key.getLangCode())) {
                        String attname = key.getAttName();
                        if (! DocAttributes.isInternalAttributeName(attname)) {
                            String val = (String) entry.getValue();
                            if ((val == null) && (lang_id == null)) {
                                val = "";   // return empty string as default value for original language
                            }
                            if (val != null) result_map.put(attname, val);
                        }
                    }
                }
                commitLocalTransaction(started);
                return result_map;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }
    

    public String[] getAttributeNames()
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                SortedSet result_set = new TreeSet();
                DbNode db_node = getDbNodeReal();
                Iterator it = db_node.getAttributes().keySet().iterator();
                while (it.hasNext()) {
                    NodeAttributeKey key = (NodeAttributeKey) it.next();
                    if (key.getLangCode().equals(LANG_ORIG)) {
                        String attname = key.getAttName();
                        if (! DocAttributes.isInternalAttributeName(attname)) {
                            result_set.add(attname);
                        }
                    }
                }
                commitLocalTransaction(started);
                return (String[]) result_set.toArray(new String[result_set.size()]);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    public String getAttribute(String name) 
    {
        if (DocConstants.DEBUG) {
            Log.info("Getting attribute: " + name);
        }
        synchronized (docSession) {
            String lang_id = docSession.getTranslationMode();
            boolean started = startLocalTransaction();
            try {
                String lang_key = (lang_id == null) ? LANG_ORIG : lang_id;  // transform null to string for database layer
                DbNode db_node = getDbNodeReal();
                String res = db_node.getAttribute(lang_key, name);
                if ((res == null) && (lang_id != null)) {  // if no translated value exists in translation mode
                    // return value of original language
                    res = db_node.getAttribute(LANG_ORIG, name);
                }
                commitLocalTransaction(started);
                return (res == null) ? "" : res;  // return empty string as default value
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    public String getAttribute(String name, String lang_id)
    {
        if (DocConstants.DEBUG) {
            Log.info("Getting attribute: " + name + ", " + lang_id);
        }
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                String lang_key = (lang_id == null) ? LANG_ORIG : lang_id;  // transform null to string for database layer
                String res = getDbNodeReal().getAttribute(lang_key, name);
                commitLocalTransaction(started);
                if ((res == null) && (lang_id == null)) {  // if value is null for original language ...
                    res = "";  // then return empty string as default value
                }
                return res;  // Note: null indicates that no translation exists
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }

    public void setAttribute(String name, String value)
    {
        setAttribute(name, value, docSession.getTranslationMode());
    }

    public void setAttribute(String name, String value, String lang_id) 
    {
        if (name == null) {
            throw new DocRuntimeException("Attribute name in setAttribute() is null!");
        }
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                DbNode nd = activateDbNode();
                prepareUpdate(nd); // allow update
                if (value == null) {
                    nd.removeAttributes(lang_id, name);  // if lang_id is null, this removes also translations
                } else {
                    String lang_str =
                        (lang_id == null) ? LANG_ORIG : lang_id; // transform null to string for database layer
                    nd.setAttribute(lang_str, name, value);
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        fireChangedEvent(lang_id);
    }

    public DocGroup getParentGroup()
    {
        if (parentGroup == null) {
            synchronized (docSession) {
                boolean started = startLocalTransaction();
                if (DocConstants.DEBUG) { 
                    Log.info("Local transaction for getParentGroup: " + started + ", " + getDocContext().getDbTransactionCounter());
                }
                try {
                    DbNode par = getDbNode().getParentNode();
                    if (par != null) {
                        parentGroup = new DocGroupDbImpl(docSession, par);
                    }
                    commitLocalTransaction(started);
                } catch (Exception ex) {
                    parentGroup = null;  // reload when getParentGroup is called next time
                    rollbackLocalTransactionRuntime(started, ex);
                }
            }
        }
        return parentGroup;
    }

    public String[] getTranslations()
    {
        Set lang_codes = null;
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                lang_codes = getAttLanguageCodes();
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        return (String[]) lang_codes.toArray(new String[lang_codes.size()]);
    }

    public boolean hasTranslation(String lang_code) 
    {
        boolean res = false;
        synchronized (docSession) {
            boolean started = startLocalTransaction();
            try {
                Iterator it = getDbNodeReal().getAttributes().keySet().iterator();
                while (it.hasNext()) {
                    NodeAttributeKey key = (NodeAttributeKey) it.next();
                    if (lang_code.equals(key.getLangCode())) {
                        res = true;
                        break;
                    }
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        return res;
    }

    public void deleteTranslation(String lang_code)
    {
        synchronized (docSession) {
            boolean started = startLocalTransaction(true);
            try {
                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                Iterator it = nd.getAttributes().keySet().iterator();
                while (it.hasNext()) {
                    NodeAttributeKey key = (NodeAttributeKey) it.next();
                    if (lang_code.equals(key.getLangCode())) it.remove();
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        fireChangedEvent(lang_code);
    }

}
