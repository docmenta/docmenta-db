/*
 * DocStoreDbConnection.java
 *
 *  Copyright (C) 2014  Manfred Paula, http://www.docmenta.org
 *   
 *  This file is part of Docmenta. Docmenta is free software: you can 
 *  redistribute it and/or modify it under the terms of the GNU Lesser 
 *  General Public License as published by the Free Software Foundation, 
 *  either version 3 of the License, or (at your option) any later version.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Docmenta.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.docma.coreapi.dbimplementation;

import java.util.*;
import java.sql.Blob;
import java.sql.SQLException;

import org.hibernate.*;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.hibernate.*;
import org.docma.util.*;

/**
 *
 * @author MP
 */
public class DocStoreDbConnection implements DocNodeContext
{
    private org.hibernate.SessionFactory dbFactory = null;
    private org.hibernate.Session dbSession = null;
    private org.hibernate.Transaction tx = null;
    private long dbSessionLastAccess = 0;
    private long dbSessionCounter = 0;
    private long dbTransactionCounter = 0;
    private Map<String, DbStore> dbStoreCache = new HashMap<String, DbStore>();

    private AbstractDocStoreManager storeManager;
    private AbstractDocStoreSession docSession;
    private VersionIdFactory versionIdFactory;
    private DocStoreDbImpl docStore;

    private String storeId = null;
    private DocVersionId versionId = null;
    private DocGroupDbImpl rootGroup;
    private DbStore openedDbStore = null;
    private DbVersion openedDbVersion = null;
    private List<DbVersion> updatedVersions = new ArrayList<DbVersion>();  // see start/commit transaction

    private Map aliasCache = new HashMap();
    // private Map storePropsCache = new HashMap();
    
    // Cache version properties to improve performance:
    private static Map<String, Map> versionPropsCache = Collections.synchronizedMap(new HashMap<String, Map>());
    private static Map<String, Long> versionPropsCacheAccessTimes = Collections.synchronizedMap(new HashMap<String, Long>());

    private static Set<Integer> cleanVersionFlags = Collections.synchronizedSet(new HashSet<Integer>()); 
    

    public DocStoreDbConnection(DbConnectionData con_data,
                                AbstractDocStoreManager dsm,
                                AbstractDocStoreSession docSess)
    {
        this(HibernateUtil.getSessionFactory(con_data), dsm, docSess);
    }
    
    public DocStoreDbConnection(org.hibernate.SessionFactory dbFactory,
                                AbstractDocStoreManager dsm,
                                AbstractDocStoreSession docSess)
    {
        this.dbFactory = dbFactory;
        this.storeManager = dsm;
        this.docSession = docSess;
        
        this.versionIdFactory = storeManager.getVersionIdFactory();
    }

    /* --------------  Private methods ------------------ */

    private void closeDbSession()
    {
        if (DocConstants.DEBUG) {
            Log.info("Closing dbSession: " + dbSessionCounter);
        }
        if (dbSession != null) {
            try {
                dbSession.close();
            } catch (Throwable th) {
                Log.warning("Closing DB session failed: " + th.getMessage());
            }
            dbSession = null;
        }
        dbStoreCache.clear();
    }

    private DbStore getDbStore(String storeId)
    {
        DbStore st = dbStoreCache.get(storeId);
        if (st == null) {
            st = DbUtil.getDbStore(dbSession, storeId);
            if (st != null) dbStoreCache.put(storeId, st);
        }
        return st;
    }

    private DbVersion getDbVersion(DbStore db_store, DocVersionId verId)
    {
        return DbUtil.getDbVersion(db_store, verId, versionIdFactory);
    }

    private DocVersionId[] listVersionIds(DbStore db_store)
    {
        Set v_set = db_store.allVersions();
        ArrayList verIds = new ArrayList(v_set.size());
        Iterator it = v_set.iterator();
        while (it.hasNext()) {
            DbVersion db_version = (DbVersion) it.next();
            String vname = db_version.getVersionName();
            try {
                verIds.add(versionIdFactory.createVersionId(vname));
            } catch (DocException ex) {
                Log.error("Invalid version name: " + vname);
            }
        }
        Collections.sort(verIds);
        DocVersionId[] verIdArr = new DocVersionId[verIds.size()];
        return (DocVersionId[]) verIds.toArray(verIdArr);
    }

    private DocVersionId getLatestVersionId(DbStore db_store)
    {
        DocVersionId[] verids = listVersionIds(db_store);
        if ((verids != null) && (verids.length > 0)) {
            return verids[verids.length - 1];
        } else {
            return null;
        }
    }

    private void setDbStoreProperties(DbStore db_store, String[] propNames, String[] propValues)
    {
        for (int i=0; i < propNames.length; i++) {
            String value = propValues[i];
            // Note: Oracle converts empty string to null. Therefore property 
            //       has to be removed if empty string value is passed.
            //       Otherwise Oracle gives contraint errors.
            if ((value == null) || (value.length() == 0)) {
                db_store.getProperties().remove(propNames[i]);
            } else {
                db_store.getProperties().put(propNames[i], value);
            }
        }
    }

    private void setDbVersionProperties(DbVersion db_ver, String[] propNames, String[] propValues)
    {
        for (int i=0; i < propNames.length; i++) {
            String value = propValues[i];
            // Note: Oracle converts empty string to null. Therefore property 
            //       has to be removed if empty string value is passed.
            //       Otherwise Oracle gives contraint errors.
            if ((value == null) || (value.length() == 0)) {
                db_ver.getProperties().remove(propNames[i]);
            } else {
                db_ver.getProperties().put(propNames[i], value);
            }
        }
    }

//    private Map getCachedStoreProps(String storeId)
//    {
//        Map props = (Map) storePropsCache.get(storeId);
//        if (props == null) {
//            // load properties from database
//            boolean started = startLocalTransaction();
//            try {
//                DbStore db_store = getDbStore(storeId);
//                props = new HashMap();
//                props.putAll(db_store.getProperties());
//                commitLocalTransaction(started);
//                storePropsCache.put(storeId, props);
//            } catch (Exception ex) {
//                rollbackLocalTransactionRuntime(started, ex);
//                return null;
//            }
//        }
//        return props;
//    }

    private synchronized Map loadVersionPropsIntoCache(String storeId, DocVersionId verId)
    {
        if (DocConstants.DEBUG) {
            Log.info("Loading version properties into cache: " + storeId + " version " + verId);
        }
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            Map props = new HashMap();
            props.putAll(db_version.getProperties());
            commitLocalTransaction(started);
            String cacheKey = versionPropsCacheKey(storeId, verId);
            versionPropsCache.put(cacheKey, props);
            versionPropsCacheAccessTimes.put(cacheKey, new Long(System.currentTimeMillis()));
            return props;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    private Map getCachedVersionProps(String storeId, DocVersionId verId)
    {
        cleanVersionPropsCache();
        String cacheKey = versionPropsCacheKey(storeId, verId);
        Map props = versionPropsCache.get(cacheKey);
        if (props != null) {
            long now = System.currentTimeMillis();
            versionPropsCacheAccessTimes.put(cacheKey, new Long(now));
        } else {
            // load properties from database
            props = loadVersionPropsIntoCache(storeId, verId);
        }
        return props;
    }
    
    private static synchronized void cleanVersionPropsCache()
    {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, Long>> it = versionPropsCacheAccessTimes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            Long aLong = entry.getValue();
            long lastAccess = (aLong == null) ? 0 : aLong.longValue();
            if ((now - lastAccess) > 2000) {  // invalidate cache after 2 seconds
                String cacheKey = entry.getKey();
                versionPropsCache.remove(cacheKey);
                it.remove();
            }
        }
    }
    
    private void clearVersionPropsCache(String storeId) 
    {
        // Quick and dirty implementation: delete cache of all stores
        // To do: Delete only cache of storeId.
        versionPropsCache.clear();
        versionPropsCacheAccessTimes.clear();
    }

    private void clearVersionPropsCache(String storeId, DocVersionId verId) 
    {
        String cacheKey = versionPropsCacheKey(storeId, verId);
        versionPropsCache.remove(cacheKey);
        versionPropsCacheAccessTimes.remove(cacheKey);
    }

    private String versionPropsCacheKey(String storeId, DocVersionId verId)
    {
        return docSession.getSessionId() + "#" + storeId + "/" + verId;
    }

    private boolean versionDependencyExists(DbVersion db_version)
    {
        int v_dbid = db_version.getVersionDbId();
        Query q = dbSession.createQuery("select dep.nodeDbId from DbNode dep, DbNode nd " +
                                        "where dep.versionDbId != :vid and dep.lastModNodeDbId = nd.nodeDbId and nd.versionDbId = :vid");
        q.setInteger("vid", v_dbid);
        q.setMaxResults(1);
        List result = q.list();
        return result.size() > 0;
    }

    private List getDbSubVersions(DbStore db_store, DbVersion baseVersion)
    {
        List subversions = new ArrayList(100);
        int base_id = baseVersion.getVersionDbId();
        Iterator it = db_store.allVersions().iterator();
        while (it.hasNext()) {
            DbVersion ver = (DbVersion) it.next();
            DbVersion v_base = ver.getBaseVersion();
            if ((v_base != null) && (v_base.getVersionDbId() == base_id)) {
                subversions.add(ver);
            }
        }
        return subversions;
    }

    private DbNode getDbNodeByAlias(String alias, DbVersion ver)
    {
        Query q = dbSession.createQuery("from DbAlias as al where al.alias = :aname and al.version.versionDbId = :verid");
        q.setString("aname", alias);
        q.setInteger("verid", ver.getVersionDbId());
        List result = q.list();
        int cnt = result.size();
        if (cnt == 0) {
            return null;
        }
        if (cnt > 1) {
            Log.warning("Alias name is not unique: " + alias);
        }
        DbNode db_node = null;
        if (cnt > 0) {
            int i = 0;
            do {
                DbAlias dba = (DbAlias) result.get(i);
                db_node = dba.getNode();
                if (db_node == null) {
                    markVersionForCleaning(ver);
                    Log.warning("Orphan alias: " + alias);
                }
            } while ((db_node == null) && (++i < cnt));
        }
        return db_node;
    }
    
    private void markVersionForCleaning()
    {
        markVersionForCleaning(openedDbVersion);
    }
    
    private void markVersionForCleaning(DbVersion ver)
    {
        if (ver == null) {
            return;
        }
        cleanVersionFlags.add(ver.getVersionDbId());
    }
    
    private void cleanMarkedVersion(DbVersion ver) 
    {
        if (ver == null) {
            return;
        }
        Integer ver_db_id = ver.getVersionDbId();
        if (cleanVersionFlags.contains(ver_db_id)) {
            try {
                String hql = "delete DbAlias a where (a.version.versionDbId = :verid) and (a.node is null)";
                Query q = dbSession.createQuery(hql);
                q.setInteger("verid", ver_db_id);
                int del_cnt = q.executeUpdate();
                cleanVersionFlags.remove(ver_db_id);
                if (del_cnt > 0) {
                    Log.info("Orphan aliases have been deleted: " + del_cnt);
                }
            } catch (Exception ex) {
                Log.error("Exception in cleanMarkedVersion: " + ex.getMessage());
            }
        }
    }
    
    private void prepareVersionUpdate(DbVersion db_ver)
    {
        int db_id = db_ver.getVersionDbId();
        for (DbVersion v : updatedVersions) {
            if (v.getVersionDbId() == db_id) return;  // version is already contained
        }
        updatedVersions.add(db_ver);
        dbSession.setReadOnly(db_ver, false);  // allow update
    }

    /* --------------  Package local methods ------------------ */

    DbStore getOpenedDbStore()
    {
        if (! dbSession.contains(openedDbStore)) {
            openedDbStore = getDbStore(this.storeId);
        }
        return openedDbStore;
    }

    DbVersion getOpenedDbVersion()
    {
        if (! dbSession.contains(openedDbVersion)) {
            openedDbVersion = getDbVersion(getOpenedDbStore(), this.versionId);
        }
        return openedDbVersion;
    }
    

    /* -----------  Methods of interface DocNodeContext --------------- */
    
    public void nodeAddedEvent(DocGroup parent, DocNode node) 
    {
        docSession.nodeAddedEvent(parent, node);
    }
    
    public void nodeRemovedEvent(DocGroup parent, DocNode node) 
    {
        docSession.nodeRemovedEvent(parent, node);
    }
    
    public void nodeChangedEvent(DocGroup parent, DocNode node, String lang)
    {
        docSession.nodeChangedEvent(parent, node, lang);
    }

    public boolean startLocalTransaction()
    {
        return startLocalTransaction(false);
    }
    
    public boolean startLocalTransaction(boolean clear_cache)
    {
        // if a transaction is not already running, then start
        // a "local" transaction
        if (runningTransaction()) {
            return false;
        } else {
            try {
                start_Transaction(clear_cache);
                return true;
            } catch (DocException dex) {
                throw new DocRuntimeException(dex);
            }
        }
    }

    public void commitLocalTransaction(boolean started)
    {
        try {
            if (started) commitTransaction();
        } catch (DocException dex) {
            throw new DocRuntimeException(dex);
        }
    }

    public void rollbackLocalTransaction(boolean started)
    {
        if (started) rollbackTransaction();
    }

    public void rollbackLocalTransactionRuntime(boolean started, Exception ex)
    {
        if (started) rollbackTransaction();
        if (ex instanceof RuntimeException) throw (RuntimeException) ex;
        else throw new DocRuntimeException(ex);
    }

    public void rollbackLocalTransactionRethrow(boolean started, Exception ex) throws DocException
    {
        if (started) rollbackTransaction();
        if (ex instanceof DocException) throw (DocException) ex;
        else throw new DocException(ex);
    }

    public org.hibernate.Session getDbSession()
    {
        return dbSession;
    }

    public long getDbSessionCounter()
    {
        return dbSessionCounter;
    }
    
    public long getDbTransactionCounter()
    {
        return dbTransactionCounter;
    }

    public synchronized void refreshAliasList()
    {
        aliasCache.clear();
    }

    public DocNodeDbImpl createDocNodeFromDbNode(DbNode db_node)
    {
        String nd_type = db_node.getNodeType();
        if (nd_type == null) {
            Log.error("Invalid node type: null");
            return null;
        }
        if (nd_type.equals(DbConstants.NODE_TYPE_GROUP)) {
            return new DocGroupDbImpl(this, db_node);
        } else
        if (nd_type.equals(DbConstants.NODE_TYPE_XML)) {
            return new DocXMLDbImpl(this, db_node);
        } else
        if (nd_type.equals(DbConstants.NODE_TYPE_IMAGE)) {
            return new DocImageDbImpl(this, db_node);
        } else
        if (nd_type.equals(DbConstants.NODE_TYPE_FILE)) {
            return new DocFileDbImpl(this, db_node);
        } else
        if (nd_type.equals(DbConstants.NODE_TYPE_REFERENCE)) {
            return new DocReferenceDbImpl(this, db_node);
        } else {
            Log.error("Invalid node type: " + nd_type);
            return null;
        }
    }

    public void activateNode(DbNode nd)
    {
        if (! nd.persisted()) {
            return;  // do nothing for transient nodes
        }
        long lastmodId = nd.getLastModNodeDbId();
        if ((lastmodId > 0) && (lastmodId != nd.getNodeDbId())) {
            if (DocConstants.DEBUG) System.out.println("Start activateNode.");
            
            DbNode nd_real = (DbNode) dbSession.load(DbNode.class, lastmodId);
            dbSession.setReadOnly(nd, false);  // allow update

            // Copy attributes to node
            Map nd_atts = nd.getAttributes();
            nd_atts.clear();
            nd_atts.putAll(nd_real.getAttributes());

            // Copy binary content
            Iterator it = nd_real.allBinaryContent().values().iterator();
            while (it.hasNext()) {
                DbBinaryContent source_cont = (DbBinaryContent) it.next();
                DbBinaryContent dest_cont = nd.createBinaryContent(source_cont.getLangCode());
                dest_cont.setContentType(source_cont.getContentType());
                dest_cont.setContentLength(source_cont.getContentLength());
                // dbSession.save(dest_cont);
                try {
                    Blob source_lob = source_cont.getNodeContent(this);
                    Blob dest_lob = dbSession.getLobHelper()
                                             .createBlob(source_lob.getBinaryStream(),
                                                         source_lob.length());
                    dest_cont.setNodeContent(dest_lob, this);
                    dbSession.flush();  // dbSession.update(dest_cont);
                } catch (SQLException sqlex) {
                    throw new DocRuntimeException(sqlex);
                }
            }

            // Copy text content
            it = nd_real.allTextContent().values().iterator();
            while (it.hasNext()) {
                DbTextContent source_cont = (DbTextContent) it.next();
                DbTextContent dest_cont = nd.createTextContent(source_cont.getLangCode());
                dest_cont.setContentType(source_cont.getContentType());
                // dest_cont.setContentLength(source_cont.getContentLength());
                // dbSession.save(dest_cont);
                dest_cont.setContent(source_cont.getContent());
                dbSession.flush();  // dbSession.update(dest_cont);
            }

            // Image renditions don't have to be copied, because they are
            // re-created dynamically if non-existent.

            nd.setLastModNodeDbId(-1);  // remove reference to last modified node
            dbSession.update(nd);
            
            if (DocConstants.DEBUG) System.out.println("End activateNode.");
        }
    }

    public DocStoreDbImpl getDocStore()
    {
        return docStore;
    }

    public synchronized void onReleaseTranslation(String storeId, DocVersionId verId, String lang)
    throws DocException
    {
        boolean started = startLocalTransaction();
        try {
            String lang_ext = "." + lang.toLowerCase();
            String[] names = {
                AbstractDocStoreSession.PROP_VERSION_STATE + lang_ext,
                AbstractDocStoreSession.PROP_VERSION_RELEASE_DATE + lang_ext };
            String[] values = { DocVersionState.DRAFT, "" };
            DocVersionId[] subs = getSubVersions(storeId, verId);
            for (int i=0; i < subs.length; i++) {
                String sub_state = docSession.getVersionState(storeId, subs[i], lang);
                if (sub_state.equalsIgnoreCase(DocVersionState.RELEASED)) {
                    // Something went wrong. Never overwrite released version!
                    throw new DocRuntimeException("Expected version state PENDING. Found RELEASED.");
                }
                if (! sub_state.equalsIgnoreCase(DocVersionState.TRANSLATION_PENDING)) {
                    Log.warning("Expected version state PENDING. Found: " + sub_state);
                }
                // change state of derived version from pending to draft
                setVersionProperties(storeId, subs[i], names, values);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and rethrow exception
        }
    }


    public synchronized void setTranslationBackToPending(String storeId, DocVersionId verId, String lang)
    throws DocException
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion db_ver = getDbVersion(getDbStore(storeId), verId);
            // String lang = getTranslationMode();
            String lang_ext = "." + lang.toLowerCase();
            String[] names = {
                AbstractDocStoreSession.PROP_VERSION_STATE + lang_ext,
                AbstractDocStoreSession.PROP_VERSION_RELEASE_DATE + lang_ext };
            String[] values = { DocVersionState.TRANSLATION_PENDING, "" };
            prepareVersionUpdate(db_ver);  // allow update
            setDbVersionProperties(db_ver, names, values);

            try {
                Query q = dbSession.createQuery("from DbNode where versionDbId = :ver_id");
                q.setInteger("ver_id", db_ver.getVersionDbId());
                q.setCacheMode(CacheMode.IGNORE);
                q.setReadOnly(false);   // allow update of returned objects
                ScrollableResults nodes = q.scroll(ScrollMode.FORWARD_ONLY);
                int count = 0;
                while ( nodes.next() ) {
                    DbNode node = (DbNode) nodes.get(0);
                    node.removeTranslation(lang);
                    if ( ++count % 25 == 0 ) {
                        //flush a batch of updates and release memory:
                        dbSession.flush();
                        // dbSession.clear();
                    }
                }
                dbSession.flush();
                // dbSession.clear();
            } catch (Exception ex) {
                ex.printStackTrace();
                Log.error("Error in setTranslationBackToPending(): " + ex.getMessage());
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and rethrow exception
        }
    }


    /* --------------  Interface DocStoreSession ------------------ */

    public String getUserId()
    {
        return docSession.getUserId();
    }
    
    public String getTranslationMode()
    {
        return docSession.getTranslationMode();
    }
    
    public String getStoreId()
    {
        return storeId;
    }

    public DocVersionId getVersionId()
    {
        return versionId;
    }

    public synchronized void openDocStore(String storeId, DocVersionId verId)
    {
        if (runningTransaction()) {
            throw new DocRuntimeException("Cannot open document store during a transaction.");
        }
        if (this.storeId != null) closeDocStore();

        boolean started = startLocalTransaction(true);
        try {
            this.storeId = storeId;
            try {
                openedDbStore = getDbStore(storeId);  // test existence of store
            } catch (Exception ex) {
                throw new DocRuntimeException("Cannot open document store '" + storeId + "'.", ex);
            }
            if (verId == null) {
                verId = getLatestVersionId(openedDbStore);
                if (verId == null) {
                    throw new DocRuntimeException("Cannot open latest version of document store '" + storeId +
                                                  "'. Version does not exist.");
                }
            }
            this.versionId = verId;
            openedDbVersion = getDbVersion(openedDbStore, verId);
            if (openedDbVersion == null) {
                throw new DocRuntimeException("Cannot open document store '" + storeId +
                                              "'. Version does not exist: " + verId);
            }

            docStore = (DocStoreDbImpl) storeManager.acquireStore(docSession, storeId, versionId);

            try {
                DbNode rootnode = (DbNode) openedDbVersion.allRootNodes().get(0);
                rootGroup = new DocGroupDbImpl(this, rootnode);
            } catch (Exception ex) {
                throw new DocRuntimeException("Missing root node.", ex);
            }
            cleanMarkedVersion(openedDbVersion);

            commitLocalTransaction(started);
        } catch (Exception ex) {
            this.storeId = null;
            this.versionId = null;
            this.docStore = null;
            this.rootGroup = null;
            this.openedDbStore = null;
            this.openedDbVersion = null;
            rollbackLocalTransactionRuntime(started, ex);  // rollback and throw runtime exception
        }
        aliasCache.clear();
        docSession.onOpenDocStore();  // add listeners to store instance
    }

    public synchronized void closeDocStore()
    {
        if (this.storeId == null) return;
        if (runningTransaction()) {
            throw new DocRuntimeException("Cannot close document store during a transaction.");
        }
        docSession.onCloseDocStore();  // remove listeners from store instance
        storeManager.releaseStore(docSession, storeId, versionId);
        storeId = null;
        versionId = null;
        docStore = null;
        rootGroup = null;
        openedDbStore = null;
        openedDbVersion = null;
        aliasCache.clear();
    }

    public synchronized String[] listDocStores()
    {
        boolean started = startLocalTransaction();
        try {
            List result = dbSession.createQuery("from DbStore").list();
            String[] ids = new String[result.size()];
            for (int i=0; i< ids.length; i++) {
                DbStore db_store = (DbStore) result.get(i);
                ids[i] = db_store.getStoreDisplayId();
            }
            commitLocalTransaction(started);
            return ids;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);  // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized void createDocStore(String storeId, String[] propNames, String[] propValues) throws DocException
    {
        boolean started = startLocalTransaction();
        try {
            DbStore db_store = new DbStore();
            db_store.setStoreDisplayId(storeId);
            dbSession.save(db_store);    // make persistent (connect db_store with session)
            dbSession.setReadOnly(db_store, false);  // allow database updates

            if (propNames != null) {
                setDbStoreProperties(db_store, propNames, propValues);
            }

            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and rethrow exception
        }
    }

    public synchronized void deleteDocStore(String storeId) throws DocException
    {
        boolean started = startLocalTransaction(true);
        try {
            // Delete in-memory representation of store
            storeManager.destroyStoreInstances(storeId); // throws runtime exception if users are still connected

            // Delete database representation of store
            DbStore db_store = getDbStore(storeId);
            dbSession.delete(db_store);  // delete cascades, i.e. referenced objects are deleted automatically
            commitLocalTransaction(started);
            
            clearVersionPropsCache(storeId);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and rethrow exception
        }
    }

    public synchronized void changeDocStoreId(String oldId, String newId) throws DocException
    {
        String[] uids = storeManager.getConnectedUsers(oldId);
        if (uids.length > 0) {
            throw new DocException("Could not change DocStore Id. Users are still connected.");
        }
        boolean started = startLocalTransaction(true);
        try {
            // Delete in-memory representation of store
            storeManager.destroyStoreInstances(oldId); // throws runtime exception if users are still connected

            // Change store-id in database
            DbStore db_store = getDbStore(oldId);
            dbSession.setReadOnly(db_store, false);  // allow database updates
            db_store.setStoreDisplayId(newId);
            commitLocalTransaction(started);

            // storePropsCache.remove(oldId);
            clearVersionPropsCache(oldId);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and rethrow exception
        }
    }

    public synchronized DocVersionId[] listVersions(String storeId)
    {
        boolean started = startLocalTransaction();
        try {
            DocVersionId[] ids = listVersionIds(getDbStore(storeId));
            commitLocalTransaction(started);
            return ids;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public synchronized DocVersionId getLatestVersionId(String storeId)
    {
        boolean started = startLocalTransaction();
        try {
            DocVersionId vid = getLatestVersionId(getDbStore(storeId));
            commitLocalTransaction(started);
            return vid;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public DocVersionId[] getRootVersions(String storeId)
    {
        DocVersionId[] all_ids = listVersions(storeId);
        ArrayList root_list = new ArrayList();
        for (int i=0; i < all_ids.length; i++) {
            if (getVersionDerivedFrom(storeId, all_ids[i]) == null) {
                root_list.add(all_ids[i]);
            }
        }
        return (DocVersionId[]) root_list.toArray(new DocVersionId[root_list.size()]);
    }

    public synchronized void deleteVersion(String storeId, DocVersionId verId) throws DocException
    {
        deleteVersion(storeId, verId, true);
    }
    
    public synchronized void deleteVersion(String storeId, DocVersionId verId, boolean verify) throws DocException
    {
        String[] uids = storeManager.getConnectedUsers(storeId, verId);
        if (uids.length > 0) {
            throw new DocException("Cannot delete version. Users are still connected.");
        }
        boolean started = startLocalTransaction(true);
        try {
            // Delete in-memory representation of version
            storeManager.destroyStoreInstance(storeId, verId); // throws runtime exception if users are still connected

            // Delete database representation of version
            DbStore db_store = getDbStore(storeId);
            DbVersion db_version = getDbVersion(db_store, verId);
            if (verify) {
                List subvers = getDbSubVersions(db_store, db_version);
                if (subvers.size() > 0) {
                    throw new DocException("Cannot delete version " + verId + ": Derived versions exist.");
                }
                if (versionDependencyExists(db_version)) {
                    throw new DocException("Cannot delete version " + verId +
                                           ": Node dependencies from other versions exist.");
                }
            }
            dbSession.setReadOnly(db_store, false);   // allow database update
            prepareVersionUpdate(db_version); // allow database update
            db_store.removeVersion(db_version);

            commitLocalTransaction(started);
            
            clearVersionPropsCache(storeId);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);  // rollback and throw runtime exception
        }
    }

    public Set<DocVersionId> deleteVersionsRecursive(String storeId, 
                                                     DocVersionId[] vers, 
                                                     boolean verify, 
                                                     ProgressCallback progress) 
                                                     throws DocException
    {
        Set<DocVersionId> deletedVers = new TreeSet<DocVersionId>();
        if ((vers != null) && (vers.length > 0)) {
            for (DocVersionId vid : vers) {
                // Delete all sub-versions before deleting the version itself
                DocVersionId[] subvers = getSubVersions(storeId, vid);
                if ((subvers != null) && (subvers.length > 0)) {
                    deletedVers.addAll(deleteVersionsRecursive(storeId, subvers, verify, progress));
                }
                if (! deletedVers.contains(vid)) {
                    if (progress != null) {
                        if (progress.getCancelFlag()) {
                            throw new DocException("Canceled by user.");
                        }
                        progress.setMessage("text.progress_delete_version", vid.toString());
                    }
                    deleteVersion(storeId, vid, verify);
                    deletedVers.add(vid);
                }
            }
        }
        return deletedVers;
    }

    public int deleteAllVersions(String storeId, ProgressCallback progress) throws DocException
    {
        DocVersionId[] rootVerIds = getRootVersions(storeId);
        if (rootVerIds.length > 0) {
            Set<DocVersionId> deleted = deleteVersionsRecursive(storeId, rootVerIds, false, progress);
            
            // Check if deletion was successful
            boolean started = startLocalTransaction(true);  // clear cache to assure reading from DB (in case of local transaction)
            try {
                DocVersionId[] vids = listVersionIds(getDbStore(storeId));
                commitLocalTransaction(started);
                if ((vids != null) && (vids.length > 0)) {
                    StringBuilder sb = new StringBuilder(vids[0].toString());
                    for (int i = 1; i < vids.length; i++) sb.append(", ").append(vids[i].toString());
                    Log.error("deleteAllVersions() was not successful. Versions still exist: " + sb);
                }
            } catch (Exception ex) {
                Log.error("Check if deleteAllVersions() was succeessful failed with exception: " +  ex.getMessage());
                ex.printStackTrace();
                rollbackLocalTransaction(started);   // silent rollback
            }
            return deleted.size();
        } else {
            return 0;
        }
    }

    public synchronized DocVersionId getVersionDerivedFrom(String storeId, DocVersionId verId)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion sub_ver = getDbVersion(getDbStore(storeId), verId);
            DbVersion base_ver = sub_ver.getBaseVersion();
            DocVersionId vid =
                (base_ver == null) ? null : versionIdFactory.createVersionId(base_ver.getVersionName());
            commitLocalTransaction(started);
            return vid;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public synchronized DocVersionId[] getSubVersions(String storeId, DocVersionId verId)
    {
        boolean started = startLocalTransaction();
        try {
            DbStore db_store = getDbStore(storeId);
            DbVersion db_basever = getDbVersion(db_store, verId);
            List subs = getDbSubVersions(db_store, db_basever);
            DocVersionId[] sub_ids = new DocVersionId[subs.size()];
            for (int i=0; i < subs.size(); i++) {
                DbVersion db_subver = (DbVersion) subs.get(i);
                sub_ids[i] = versionIdFactory.createVersionId(db_subver.getVersionName());
            }
            commitLocalTransaction(started);
            return sub_ids;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public synchronized void renameVersion(String storeId, DocVersionId oldVerId, DocVersionId newVerId)
    throws DocException
    {
        boolean started = startLocalTransaction(true);
        try {
            docSession.checkRenameVersion(storeId, oldVerId, newVerId);

            // Delete in-memory representation of version
            storeManager.destroyStoreInstance(storeId, oldVerId); // throws runtime exception if users are still connected

            DbVersion db_ver = getDbVersion(getDbStore(storeId), oldVerId);
            prepareVersionUpdate(db_ver);  // allow update
            db_ver.setVersionName(newVerId.toString());
            commitLocalTransaction(started);
            
            clearVersionPropsCache(storeId);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    private DbNode deriveNode(DbNode baseNode, DbVersion newVersion)
    {
        DbNode newNode = newVersion.createNode(baseNode.getNodeNumber());
        newNode.setNodeType(baseNode.getNodeType());
        newNode.setAliases(baseNode.getAliases());
        long baseModId = baseNode.getLastModNodeDbId();
        if (baseModId < 0) {
            // the attributes/contents are stored in this node
            newNode.setLastModNodeDbId(baseNode.getNodeDbId());
        } else {
            // the attributes/contents of the base node are derived from a previous version
            newNode.setLastModNodeDbId(baseModId);
        }
        return newNode;
    }

    private void deriveNodesRecursive(DbNode sourceNode, DbNode destNode, DbVersion newVersion)
    {
        List children = sourceNode.allChildNodes();
        for (int i=0; i < children.size(); i++) {
            DbNode child = (DbNode) children.get(i);
            DbNode childcopy = deriveNode(child, newVersion);
            destNode.addChildNode(childcopy);
            dbSession.flush(); // dbSession.save(childcopy);
            dbSession.setReadOnly(childcopy, false);  // allow adding of child nodes
            deriveNodesRecursive(child, childcopy, newVersion);
            dbSession.evict(child);       // remove from cache to avoid out of memory
            dbSession.evict(childcopy);   // remove from cache to avoid out of memory
        }
        // dbSession.flush(); // flush not needed, because id generation disables batch inserts
        // List destChildren = destNode.allChildNodes();
        // for (int i=0; i < children.size(); i++) {
        //     DbNode child = (DbNode) children.get(i);
        //     DbNode childcopy = (DbNode) destChildren.get(i);
        //     if (child.getNodeNumber() != childcopy.getNodeNumber()) {
        //         throw new DocRuntimeException("Fatal error: Node number mismatch!");
        //     }
        //     deriveNodesRecursive(child, childcopy, newVersion);
        //     dbSession.evict(child);       // remove from cache to avoid out of memory
        //     dbSession.evict(childcopy);   // remove from cache to avoid out of memory
        // }
        // dbSession.clear();
    }

    public synchronized void createVersion(String storeId, DocVersionId baseVersion, DocVersionId newVersion)
    throws DocException
    {
        boolean started = startLocalTransaction(true);
        try {
            docSession.checkCreateVersion(storeId, baseVersion, newVersion);

            DbStore db_store = getDbStore(storeId);
            DbVersion db_newver = new DbVersion();
            db_newver.setVersionName(newVersion.toString());

            // Copy fields from base version to new version
            Map newprops = db_newver.getProperties();
            DbVersion db_basever = null;
            if (baseVersion != null) {
                db_basever = getDbVersion(db_store, baseVersion);
                db_newver.setBaseVersion(db_basever);
                db_newver.setNextNodeNumber(db_basever.getNextNodeNumber());
                newprops.putAll(db_basever.getProperties());  // copy properties from base version

                // Change state of released translations to draft for the new version
                final String PREFIX = AbstractDocStoreSession.PROP_VERSION_STATE + ".";
                Object[] pnames = newprops.keySet().toArray();
                for (Object key : pnames) {
                    String propname = key.toString();
                    if (propname.startsWith(PREFIX)) {
                        String lang_code = propname.substring(PREFIX.length());
                        String state_val = (String) newprops.get(propname);
                        if (DocVersionState.RELEASED.equalsIgnoreCase(state_val)) {
                            newprops.put(propname, DocVersionState.DRAFT);
                        } else {
                            newprops.put(propname, DocVersionState.TRANSLATION_PENDING);
                        }
                        // delete release date
                        newprops.remove(AbstractDocStoreSession.PROP_VERSION_RELEASE_DATE + "." + lang_code);
                        // Note: Oracle converts empty string to null! Therefore remove property
                        //       instead of setting an empty string.
                    }
                }

                // Remove UUID of base version -> new UUID will be assigned
                newprops.remove(AbstractDocStoreSession.PROP_VERSION_UUID);
                // Note: Oracle converts empty string to null! Therefore remove property
                //       instead of setting an empty string.
            }
            newprops.put(AbstractDocStoreSession.PROP_VERSION_STATE, DocVersionState.DRAFT);
            newprops.put(AbstractDocStoreSession.PROP_VERSION_CREATION_DATE, "" + System.currentTimeMillis());
            newprops.remove(AbstractDocStoreSession.PROP_VERSION_RELEASE_DATE);
            // Note: Oracle converts empty string to null! Therefore remove property
            //       instead of setting an empty string.

            dbSession.setReadOnly(db_store, false);  // allow update
            db_store.addVersion(db_newver);
            dbSession.save(db_newver);  // save new version
            // dbSession.flush();  // flush not needed, because id generation disables batch inserts

            if (baseVersion != null) {
                // If version is derived from base version, then
                // copy all nodes from base version to new version (recursively)
                List roots = db_basever.allRootNodes();
                for (int i=0; i < roots.size(); i++) {
                    DbNode root = (DbNode) roots.get(i);
                    DbNode rootcopy = deriveNode(root, db_newver);
                    db_newver.addRootNode(rootcopy);
                    dbSession.save(rootcopy);
                    dbSession.setReadOnly(rootcopy, false);  // allow adding of child nodes
                    deriveNodesRecursive(root, rootcopy, db_newver);
                }
            } else {
                // Create initial root group.
                // Note that initial root group will get node number 1 (as it is the first node).
                // This has to be consistent between different store implementations, 
                // to be able to create exact store copies.
                DbNode new_root = db_newver.createNode();
                new_root.setNodeType(DbConstants.NODE_TYPE_GROUP);
                db_newver.addRootNode(new_root);
                dbSession.save(new_root);
            }

            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    public DocGroup getRoot()
    {
        return rootGroup;
    }

    public synchronized DocNode getNode(String idOrAlias)
    {
        DocNode doc_node = null;
        boolean started = startLocalTransaction();
        try {
            doc_node = getNodeById(idOrAlias);
            if (doc_node == null) {
                doc_node = getNodeByAlias(idOrAlias);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransaction(started);  // rollback transaction and ignore exception
        }
        return doc_node;
    }

    public synchronized boolean nodeIdExists(String id)
    {
        long node_num;
        try {
            node_num = DbUtil.parseNodeNumber(id);
        } catch (Exception ex) {
            return false; // no valid node number
        }
        Number count = null;
        boolean started = startLocalTransaction();
        try {
            DbVersion ver = getOpenedDbVersion();
            Query q = dbSession.createQuery("select count(*) from DbNode where nodeNumber = :node_num and version.versionDbId = :ver_id");
            q.setLong("node_num", node_num);
            q.setInteger("ver_id", ver.getVersionDbId());
            count = (Number) q.uniqueResult();
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);  // rollback transaction, throw runtime exception
        }
        return (count.longValue() > 0);
    }

    public synchronized DocNode getNodeById(String id)
    {
        DbNode db_node = null;
        boolean started = startLocalTransaction();
        try {
            long node_num = DbUtil.parseNodeNumber(id);
            DbVersion ver = getOpenedDbVersion();
            Query q = dbSession.createQuery("from DbNode where nodeNumber = :node_num and version.versionDbId = :ver_id");
            q.setLong("node_num", node_num);
            q.setInteger("ver_id", ver.getVersionDbId());
            db_node = (DbNode) q.uniqueResult();
        } catch (Exception ex) {
            // No valid node number. Either parseNodeNumber() or uniqueResult()
            // gave an exception. db_node remains null, i.e. result is null.
        }
        DocNode res = (db_node == null) ? null : createDocNodeFromDbNode(db_node);
        try {
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransaction(started);  // rollback transaction and ignore exception
        }
        return res;
    }

    public synchronized DocNode getNodeByAlias(String alias) 
    {
        DocNode res = null;
        boolean started = startLocalTransaction();
        try {
            DbNode db_node = getDbNodeByAlias(alias, getOpenedDbVersion());
            if (db_node != null) {
                res = createDocNodeFromDbNode(db_node);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransaction(started);  // rollback transaction and ignore exception
        }
        return res;
    }

    public synchronized String getNodeIdByAlias(String alias) 
    {
        boolean started = startLocalTransaction();
        try {
            DbNode db_node = getDbNodeByAlias(alias, getOpenedDbVersion());
            long db_num = db_node.getNodeNumber();
            commitLocalTransaction(started);
            return DbUtil.formatNodeNumber(db_num);
        } catch (Exception ex) {
            rollbackLocalTransaction(started);  // rollback transaction and ignore exception
            return null;
        }
    }
    
    private String getFilterConditionByClass(Class node_class)
    {
        String condition;
        if (node_class == DocAtom.class) {
            condition = "node.nodeType in ('" +
                        DbConstants.NODE_TYPE_XML + "', '" +
                        DbConstants.NODE_TYPE_IMAGE + "', '" +
                        DbConstants.NODE_TYPE_FILE + "', '" +
                        DbConstants.NODE_TYPE_REFERENCE + "')";
        } else
        if (node_class == DocContent.class) {
            condition = "node.nodeType in ('" +
                        DbConstants.NODE_TYPE_XML + "', '" +
                        DbConstants.NODE_TYPE_IMAGE + "', '" +
                        DbConstants.NODE_TYPE_FILE + "')";
        } else
        if (node_class == DocGroup.class) {
            condition = "node.nodeType = '" + DbConstants.NODE_TYPE_GROUP + "'";
        } else
        if (node_class == DocXML.class) {
            condition = "node.nodeType = '" + DbConstants.NODE_TYPE_XML + "'";
        } else
        if (node_class == DocImage.class) {
            condition = "node.nodeType = '" + DbConstants.NODE_TYPE_IMAGE + "'";
        } else
        if (node_class == DocFile.class) {
            condition = "node.nodeType = '" + DbConstants.NODE_TYPE_FILE + "'";
        } else
        if (node_class == DocReference.class) {
            condition = "node.nodeType = '" + DbConstants.NODE_TYPE_REFERENCE + "'";
        } else
        if (node_class == DocNode.class) {
            condition = null;
        } else {
            throw new DocRuntimeException("Cannot list aliases. Invalid node class: " + node_class.getName());
        }
        return condition;
    }

    public synchronized String[] listIds(Class node_class)
    {
        if (rootGroup == null) {
            throw new DocRuntimeException("Method listIds() can only be called on an open document store!");
        }
        if (node_class == null) node_class = DocNode.class;
        
        String condition = getFilterConditionByClass(node_class);
        List result = null;
        boolean started = startLocalTransaction();
        try {
            DbVersion db_ver = getOpenedDbVersion();
            condition = (condition == null) ? "" : (" and " + condition);
            result = dbSession.createQuery(
                "select node.nodeNumber from DbNode node where node.versionDbId = " + 
                db_ver.getVersionDbId() + condition).list();
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        if (result != null) {
            // Collections.sort(result_list);
            for (int i=0; i < result.size(); i++) {
                long node_num = ((Number) result.get(i)).longValue();
                result.set(i, DbUtil.formatNodeNumber(node_num));
            }
            String[] id_arr = (String[]) result.toArray(new String[result.size()]);
            // Arrays.sort(id_arr);
            return id_arr;
        } else {
            return new String[0];
        }
    }
    
    public synchronized String[] listAliases(Class node_class)
    {
        if (rootGroup == null) {
            throw new DocRuntimeException("Method listAliases() can only be called on an open document store!");
        }
        if (node_class == null) node_class = DocNode.class;

        String[] aliases = (String[]) aliasCache.get(node_class.getName());
        if (aliases != null) return aliases;  // return cached list

        // read alias list from database
        String condition = getFilterConditionByClass(node_class);
        List result = null;
        boolean started = startLocalTransaction();
        try {
            DbVersion db_ver = getOpenedDbVersion();
            condition = (condition == null) ? "" : (" and al." + condition);
            result = dbSession.createQuery(
                "select al.alias from DbAlias al where al.version.versionDbId = " + 
                db_ver.getVersionDbId() + condition).list();
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        if (result != null) {
            // Collections.sort(result_list);
            aliases = (String[]) result.toArray(new String[result.size()]);
            Arrays.sort(aliases);
            aliasCache.put(node_class.getName(), aliases);
            return aliases;
        } else {
            return new String[0];
        }
    }

    public synchronized List<NodeInfo> listNodeInfos(Class node_class)
    {
        if (rootGroup == null) {
            throw new DocRuntimeException("Method listIds() can only be called on an open document store!");
        }
        if (node_class == null) node_class = DocNode.class;
        
        ArrayList<NodeInfo> info_list = new ArrayList<NodeInfo>(2500);
        String condition = getFilterConditionByClass(node_class);
        boolean started = startLocalTransaction();
        try {
            DbVersion db_ver = getOpenedDbVersion();
            condition = (condition == null) ? "" : (" and " + condition);
            Query a_query = dbSession.createQuery(
                 "select node from DbNode node " + 
                 "left join fetch node.aliasList " + 
                 // "left join fetch node.attributes " +   // Oracle gives error (left outer join not allowed for type Long?) 
                 "left join fetch node.lastModifiedNode " +
                 "where node.versionDbId = " + db_ver.getVersionDbId() + condition);
            // a_query.setMaxResults(1024*1024);
            List result = a_query.list();
            if (result != null) {
                Set<Long> id_set = new HashSet(5000);
                for (Object obj : result) {
                    DbNode node = (DbNode) obj;
                    if (id_set.add(node.getNodeDbId())) {  // remove doublicated objects from query result
                        info_list.add(new NodeInfoDbImpl(node, this));
                    }
                }
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        return info_list;
    }
    
    public DocGroup createGroup()
    {
        return createGroup(null);
    }

    public synchronized DocGroup createGroup(String node_id)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion dbver = getOpenedDbVersion();
            prepareVersionUpdate(dbver);  // allow update of version (increase of nextNodeNumber)
            DbNode nd;
            if (node_id == null) {
                nd = dbver.createNode();
            } else {
                nd = dbver.createNode(Long.parseLong(node_id));
            }
            nd.setNodeType(DbConstants.NODE_TYPE_GROUP);
            DocGroup res = new DocGroupDbImpl(this, nd);
            commitLocalTransaction(started);
            return res;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public DocXML createXML()
    {
        return createXML(null);
    }

    public synchronized DocXML createXML(String node_id)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion dbver = getOpenedDbVersion();
            prepareVersionUpdate(dbver);  // allow update of version (increase of nextNodeNumber)
            DbNode nd;
            if (node_id == null) {
                nd = dbver.createNode();
            } else {
                nd = dbver.createNode(Long.parseLong(node_id));
            }
            nd.setNodeType(DbConstants.NODE_TYPE_XML);
            DocXML res = new DocXMLDbImpl(this, nd);
            commitLocalTransaction(started);
            return res;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public DocImage createImage()
    {
        return createImage(null);
    }

    public synchronized DocImage createImage(String node_id)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion dbver = getOpenedDbVersion();
            prepareVersionUpdate(dbver);  // allow update of version (increase of nextNodeNumber)
            DbNode nd;
            if (node_id == null) {
                nd = dbver.createNode();
            } else {
                nd = dbver.createNode(Long.parseLong(node_id));
            }
            nd.setNodeType(DbConstants.NODE_TYPE_IMAGE);
            DocImage res = new DocImageDbImpl(this, nd);
            commitLocalTransaction(started);
            return res;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public DocFile createFile()
    {
        return createFile(null);
    }

    public synchronized DocFile createFile(String node_id)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion dbver = getOpenedDbVersion();
            prepareVersionUpdate(dbver);  // allow update of version (increase of nextNodeNumber)
            DbNode nd;
            if (node_id == null) {
                nd = dbver.createNode();
            } else {
                nd = dbver.createNode(Long.parseLong(node_id));
            }
            nd.setNodeType(DbConstants.NODE_TYPE_FILE);
            DocFile res = new DocFileDbImpl(this, nd);
            commitLocalTransaction(started);
            return res;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }

    public DocReference createReference()
    {
        return createReference(null);
    }

    public synchronized DocReference createReference(String node_id)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion dbver = getOpenedDbVersion();
            prepareVersionUpdate(dbver);  // allow update of version (increase of nextNodeNumber)
            DbNode nd;
            if (node_id == null) {
                nd = dbver.createNode();
            } else {
                nd = dbver.createNode(Long.parseLong(node_id));
            }
            nd.setNodeType(DbConstants.NODE_TYPE_REFERENCE);
            DocReference res = new DocReferenceDbImpl(this, nd);
            commitLocalTransaction(started);
            return res;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }


    public void startTransaction() throws DocException
    {
        start_Transaction(true);
        // Note: Clear cache on start of user transaction, to avoid reading outdated data. 
    }
    
    private synchronized void start_Transaction(boolean clear_cache) throws DocException
    {
        if (tx != null) {
            // A transaction is currently running.
            // Maybe concurrent transaction was started in another thread.
            // Wait up to 1 second for the other transaction to be finished.
            if (DocConstants.DEBUG) {
                Log.info("Waiting for concurrent transaction to be finished.");
            }
            for (int i=0; i < 10; i++) {
                if (tx != null) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception ex) {
                        // ignore
                        Log.warning("Exception in Thread.sleep: " + ex.getMessage());
                    }
                } else {
                    break;
                }
            }
            if (tx != null) {
                throw new DocException("Cannot start transaction: transaction is running.");
            }
        }
        if (clear_cache && (dbSession != null)) { 
            closeDbSession();
        }
        long now = System.currentTimeMillis();
        if (dbSession != null) {
            final long SESS_TIMEOUT = 1000;
            if ((now - dbSessionLastAccess) > SESS_TIMEOUT) {
                closeDbSession();
            }
        }
        try {
            if (dbSession == null) {
                dbSession = dbFactory.openSession();
                dbSessionCounter++;
                dbSession.setDefaultReadOnly(true);
            }
            tx = dbSession.beginTransaction();
            dbTransactionCounter++;
            if (DocConstants.DEBUG) {
                Log.info("Starting DB transaction " + dbTransactionCounter + ". Clear cache: " + clear_cache);
            }
        } catch (Exception ex) {
            // if (dbSession != null) closeDbSession();
            throw new DocException(ex);
        }
        dbSessionLastAccess = now;
        updatedVersions.clear();  // allow tracking if version has been updated within the transaction (see commit)
    }

    public void commitTransaction() throws DocException
    {
        if (DocConstants.DEBUG) {
            // Log.info("Commiting DB transaction " + dbTransactionCounter);
        }
        if (tx == null) {
            throw new DocException("Cannot commit transaction: no transaction running.");
        }
        if (openedDbVersion != null) {
            openedDbVersion.deleteOrphans(dbSession);
        }
        for (DbVersion v : updatedVersions) {
            // If a version object is updated, then assure that always the 
            // highest registered node number is written to the database.
            // Otherwise, if a node is created in transaction A and another
            // node is created in transaction B, but A is commited after B,
            // then the version object may be overwritten with an outdated value
            // of nextNodeNumber.
            long v_next = v.getNextNodeNumber();
            long registered_next = NodeNumberRegistry.nextNodeNumberOfVersion(v);
            if (v_next < registered_next) {
                v.setNextNodeNumber(registered_next);
                if (DocConstants.DEBUG) {
                    Log.info("Concurrent update of nextNodeNumber in concurrent transactions.");
                }
            }
        }
        updatedVersions.clear();
        try {
            tx.commit();
            tx = null;
            // closeDbSession();
        } catch (Exception ex) {
            if (DocConstants.DEBUG) {
                ex.printStackTrace();
            }
            throw new DocException(ex);
        }
        if (docStore != null) {
            // Note that during dispatch of event queue further read operations 
            // may be executed. Therefore the dispatchEventQueue() method
            // might cause further (local) transactions, which leads to nested 
            // dispatchEventQueue() calls. However, nested calls of 
            // dispatchEventQueue() are ignored. Otherwise this may
            // cause infinite loop (because event might not be removed before 
            // event has been completeley processed).
            try {
                docStore.dispatchEventQueue(); 
            } catch (Throwable ex) {
                Log.error("Exception in commitTransaction during dispatchEventQueue(): " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    public void rollbackTransaction()
    {
        if (tx != null) {
            if (DocConstants.DEBUG) {
                Log.info("Rolling back transaction!");
            }
            try {
                tx.rollback();
                if (docStore != null) {
                    docStore.discardEventQueue();
                }
            } catch (Exception ex) {
                // throw new DocRuntimeException(ex);
                ex.printStackTrace();
                Log.warning("Could not rollback transaction: " + ex.getMessage());
            } finally {
                tx = null;
                markVersionForCleaning();
                closeDbSession();
                updatedVersions.clear();
            }
        } else {
            Log.warning("Cannot rollback transaction: no transaction running.");
        }
    }

    public synchronized boolean runningTransaction()
    {
        return (tx != null);
    }

    public synchronized void close()
    {
        if (DocConstants.DEBUG) {
            Log.info("Closing DocStoreDbConnection.");
        }
        if (this.dbSession != null) closeDbSession();
        if (this.storeId != null) closeDocStore();
    }

    public synchronized String getDocStoreProperty(String storeId, String name) 
    {
        if (DocConstants.DEBUG) {
            Log.info("Getting DB store property for store " + storeId + ": " + name);
        }
        String res = null;
        boolean started = startLocalTransaction();
        try {
            DbStore db_store = getDbStore(storeId);
            if (db_store == null) {
                throw new DocException("Store with id '" + storeId + "' not found.");
            }
            res = (String) db_store.getProperties().get(name);
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        return res;
    }

    public synchronized void setDocStoreProperty(String storeId, String name, String value) throws DocException
    {
        // storePropsCache.remove(storeId);  // clear cached properties
        boolean started = startLocalTransaction(true);
        try {
            DbStore db_store = getDbStore(storeId);
            dbSession.setReadOnly(db_store, false);  // allow update
            // Note: Oracle converts empty string to null. Therefore property 
            //       has to be removed if empty string value is passed.
            //       Otherwise Oracle gives contraint errors.
            if ((value == null) || (value.length() == 0)) {
                db_store.getProperties().remove(name);
            } else {
                db_store.getProperties().put(name, value);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    public synchronized void setDocStoreProperties(String storeId, String[] names, String[] values) throws DocException
    {
        // storePropsCache.remove(storeId);  // clear cached properties
        boolean started = startLocalTransaction(true);
        try {
            DbStore db_store = getDbStore(storeId);
            dbSession.setReadOnly(db_store, false);  // allow update
            setDbStoreProperties(db_store, names, values);
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    public synchronized String[] getDocStorePropertyNames(String storeId)
    {
        ArrayList names = new ArrayList();
        boolean started = startLocalTransaction();
        try {
            DbStore db_store = getDbStore(storeId);
            Iterator it = db_store.getProperties().keySet().iterator();
            while (it.hasNext()) {
                String nm = (String) it.next();
                if (! AbstractDocStoreSession.isInternalStoreProperty(nm)) names.add(nm);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        return (String[]) names.toArray(new String[names.size()]);
    }

    public synchronized String getVersionProperty(String storeId, DocVersionId verId, String name)
    {
        // if (DocConstants.DEBUG) {
        //     Log.info("Getting DB version property for store " + storeId + " version " + verId + ": " + name);
        // }
        Map vprops = getCachedVersionProps(storeId, verId);
        if (vprops != null) {
            String val = (String) vprops.get(name);
            return (val == null) ? "" : val;
        } else { 
            return null; 
        }
        
        // String res = null;
        // boolean started = startLocalTransaction();
        // try {
        //     DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
        //     res = (String) db_version.getProperties().get(name);
        //      commitLocalTransaction(started);
        // } catch (Exception ex) {
        //     rollbackLocalTransactionRuntime(started, ex);
        // }
        // return res;
    }

    public synchronized void setVersionProperty(String storeId, DocVersionId verId, String name, String value) throws DocException
    {
        clearVersionPropsCache(storeId, verId);  // clear cached properties
        boolean started = startLocalTransaction(true);
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            prepareVersionUpdate(db_version);  // allow update
            // Note: Oracle converts empty string to null. Therefore property 
            //       has to be removed if empty string value is passed.
            //       Otherwise Oracle gives contraint errors.
            if ((value == null) || (value.length() == 0)) {
                db_version.getProperties().remove(name);
            } else {
                db_version.getProperties().put(name, value);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    public synchronized void setVersionProperties(String storeId, DocVersionId verId, String[] names, String[] values) throws DocException
    {
        clearVersionPropsCache(storeId, verId);  // clear cached properties
        boolean started = startLocalTransaction(true);
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            prepareVersionUpdate(db_version);  // allow update
            setDbVersionProperties(db_version, names, values);
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRethrow(started, ex);
        }
    }

    public synchronized String[] getVersionPropertyNames(String storeId, DocVersionId verId)
    {
        ArrayList names = new ArrayList();
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            Iterator it = db_version.getProperties().keySet().iterator();
            while (it.hasNext()) {
                String nm = (String) it.next();
                if (! AbstractDocStoreSession.isInternalVersionProperty(nm)) names.add(nm);
            }
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
        return (String[]) names.toArray(new String[names.size()]);
    }

    //
    // *********** Content revision methods ***********
    //
    
    public synchronized DocContentRevision[] getRevisions(String storeId, 
                                                          DocVersionId verId,
                                                          String nodeId, 
                                                          String langCode)
    {
        long nodeNum = DbUtil.parseNodeNumber(nodeId);
        String langStr = (langCode == null) ? DbConstants.LANG_ORIG : langCode;
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            Query q = dbSession.createQuery(
                    "from DbContentRevision where versionDbId = " + db_version.getVersionDbId() +
                    " and nodeNumber = " + nodeNum + 
                    " and langCode = :lang order by timestamp");
            q.setString("lang", langStr);
            List result = q.list();
            DocContentRevision[] arr = null;
            if (result != null) {
                arr = new DocContentRevision[result.size()];
                for (int i = 0; i < arr.length; i++) {
                    DbContentRevision db_rev = (DbContentRevision) result.get(i);
                    arr[i] = new ContentRevisionDbImpl(this, db_rev.getRevisionDbId(), 
                                                       new Date(db_rev.getTimestamp()), 
                                                       db_rev.getUserId());
                }
            }
            commitLocalTransaction(started);
            return arr;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }
    
    public synchronized SortedSet<String> getRevisionNodeIds(String storeId, 
                                                             DocVersionId verId)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            Query q = dbSession.createQuery(
                    "select rev.nodeNumber from DbContentRevision rev where versionDbId = " + db_version.getVersionDbId());
            List result = q.list();
            SortedSet<String> idset = new TreeSet<String>();;
            if (result != null) {
                for (Object obj : result) {
                    if (obj instanceof Number) {
                        idset.add(DbUtil.formatNodeNumber(((Number) obj).longValue()));
                    }
                }
            }
            commitLocalTransaction(started);
            return idset;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return null;
        }
    }
    
    public synchronized int deleteRevisions(String storeId, DocVersionId verId, String nodeId)
    {
        long nodeNum = DbUtil.parseNodeNumber(nodeId);
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            Query q = dbSession.createQuery(
                    "from DbContentRevision where versionDbId = " + db_version.getVersionDbId() +
                    " and nodeNumber = " + nodeNum);
            Iterator it = q.iterate();
            int del_count = 0;
            while (it.hasNext()) {
                DbContentRevision del_rev = (DbContentRevision) it.next();
                dbSession.setReadOnly(del_rev, false);
                dbSession.delete(del_rev);
                del_count++;
            }
            if (DocConstants.DEBUG) {
                Log.info("Deleted " + del_count + " revisions of node '" + nodeId + 
                         "' in version '" + verId + "' of store '" + storeId + "'.");
            }
            commitLocalTransaction(started);
            return del_count;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return 0;
        }
    }

    public int clearRevisions(String storeId, DocVersionId verId)
    {
        final int MAX_LOOPS = 2;
        for (int i=1; i <= MAX_LOOPS; i++) {
            try {
                return clear_Revisions(storeId, verId);
            } catch (Exception ex) {
                if (i == MAX_LOOPS) throw new DocRuntimeException(ex);
            }
        }
        return 0;  // never reached
    }
    
    public synchronized int clear_Revisions(String storeId, DocVersionId verId)
    {
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            // Query q1 = dbSession.createQuery(
            //         "delete from DbRevisionLob r where r.owner.versionDbId = " + db_version.getVersionDbId());
            // int del1_count = q1.executeUpdate();
            // Query q2 = dbSession.createQuery(
            //         "delete from DbContentRevision where versionDbId = " + db_version.getVersionDbId());
            // int del2_count = q2.executeUpdate();
            Query q = dbSession.createQuery(
                    "from DbContentRevision where versionDbId = " + db_version.getVersionDbId());
            Iterator it = q.iterate();
            int del_count = 0;
            while (it.hasNext()) {
                DbContentRevision del_rev = (DbContentRevision) it.next();
                dbSession.setReadOnly(del_rev, false);
                dbSession.delete(del_rev);
                del_count++;
            }
            if (DocConstants.DEBUG) {
                Log.info("Cleared " + del_count + " revision entries in version '" + 
                         verId + "' of store '" + storeId + "'.");
            }
            commitLocalTransaction(started);
            return del_count;
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
            return 0;
        }
    }

    public synchronized void addRevision(String storeId,
                                         DocVersionId verId,
                                         String nodeId,
                                         String langCode,
                                         byte[] content,
                                         Date revDate,
                                         String userId, 
                                         int maxRevisionsPerUser)
    {
        long nodeNum = DbUtil.parseNodeNumber(nodeId);
        String langStr = (langCode == null) ? DbConstants.LANG_ORIG : langCode;
        boolean started = startLocalTransaction();
        try {
            DbVersion db_version = getDbVersion(getDbStore(storeId), verId);
            
            // Delete old revisions
            try {
                Query q = dbSession.createQuery(
                        "from DbContentRevision where versionDbId = " + db_version.getVersionDbId() +
                        " and nodeNumber = " + nodeNum + 
                        " and langCode = :lang and userId = :usr order by timestamp desc");
                q.setString("lang", langStr);
                q.setString("usr", userId);
                List result = q.list();
                if (result.size() > maxRevisionsPerUser) {
                    for (int i = maxRevisionsPerUser; i < result.size(); i++) {
                        DbContentRevision del_rev = (DbContentRevision) result.get(i);
                        dbSession.setReadOnly(del_rev, false);
                        dbSession.delete(del_rev);
                    }
                }
            } catch (Exception ex) {
                Log.error("Could not delete old revisions: " + ex.getMessage());
            }
            // Create new revision
            DbContentRevision db_rev = 
              new DbContentRevision(db_version.getVersionDbId(), nodeNum, langStr, revDate.getTime(), userId);
            db_rev.setContent(content);
            dbSession.save(db_rev);
            
            commitLocalTransaction(started);
        } catch (Exception ex) {
            rollbackLocalTransactionRuntime(started, ex);
        }
    }

}
