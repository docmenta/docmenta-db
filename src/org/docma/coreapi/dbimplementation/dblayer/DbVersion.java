/*
 * DbVersion.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;
import org.docma.coreapi.DocConstants;
import org.docma.util.Log;
import org.hibernate.Session;

/**
 *
 * @author MP
 */
public class DbVersion
{
    private int versionDbId = 0;   // primary key; generated value; 0 means transient
    private String versionName;
    private long nextNodeNumber = 1;

    // private int storeDbId;
    private DbStore store;

    // private int baseVersionDbId;
    private DbVersion baseVersion = null;

    private Set publicationExports = new LinkedHashSet();
    private Map properties = new HashMap();
    private List rootNodes = new ArrayList();
    private List aliases = null;

    private List<DbAlias> removedAliases = null;
    // private int lastRemovedVerDbId = -1;
    private List<DbNode> removedNodes = null;
    

    public DbVersion()
    {
    }


    public DbVersion getBaseVersion()
    {
        return baseVersion;
    }

    public void setBaseVersion(DbVersion baseVersion)
    {
        this.baseVersion = baseVersion;
    }


    public DbStore getStore()
    {
        return store;
    }

    void setStore(DbStore store)
    {
        this.store = store;
    }


    public int getVersionDbId()
    {
        return versionDbId;
    }

    void setVersionDbId(int versionDbId)
    {
        this.versionDbId = versionDbId;
    }

    public boolean persisted()
    {
        return this.versionDbId > 0;
    }

    public String getVersionName()
    {
        return versionName;
    }

    public void setVersionName(String versionName)
    {
        this.versionName = versionName;
    }


    public long getNextNodeNumber()
    {
        return nextNodeNumber;
    }

    public void setNextNodeNumber(long nextNodeNum)
    {
        this.nextNodeNumber = nextNodeNum;
    }


    public Map getProperties()
    {
        return properties;
    }

    public void setProperties(Map properties)
    {
        this.properties = properties;
    }


    Set getPublicationExports()
    {
        return publicationExports;
    }

    void setPublicationExports(Set exports)
    {
        this.publicationExports = exports;
    }

    public Set allPublicationExports()
    {
        return Collections.unmodifiableSet(this.publicationExports);
    }

    public DbPublicationExport getPublicationExport(String exportName)
    {
        DbPublicationExport found = null;
        Iterator it = getPublicationExports().iterator();
        while (it.hasNext()) {
            DbPublicationExport export = (DbPublicationExport) it.next();
            if (exportName.equals(export.getExportName())) {
                found = export;
                break;
            }
        }
        return found;
    }

    public void addPublicationExport(DbPublicationExport export)
    {
        export.setVersion(this);
        getPublicationExports().add(export);
    }

    public boolean removePublicationExport(DbPublicationExport export)
    {
        boolean removed = getPublicationExports().remove(export);
        if (removed) {
            export.setVersion(null);
        }
        return removed;
    }

    public boolean removePublicationExport(String exportName)
    {
        boolean removed = false;
        Iterator it = getPublicationExports().iterator();
        while (it.hasNext()) {
            DbPublicationExport export = (DbPublicationExport) it.next();
            if (exportName.equals(export.getExportName())) {
                it.remove();
                removed = true;
                export.setVersion(null);
                break;
            }
        }
        return removed;
    }

    public int removeAllPublicationExports()
    {
        int cnt = 0;
        Iterator it = getPublicationExports().iterator();
        while (it.hasNext()) {
            DbPublicationExport export = (DbPublicationExport) it.next();
            it.remove();
            cnt++;
            export.setVersion(null);
        }
        return cnt;
    }


    List getRootNodes()
    {
        return rootNodes;
    }

    void setRootNodes(List rootNodes)
    {
        this.rootNodes = rootNodes;
    }

    public List allRootNodes()
    {
        clearPersistedOrphans();
        return Collections.unmodifiableList(this.rootNodes);
    }

    public void addRootNode(DbNode node)
    {
        addRootNode(getRootNodes().size(), node);  // add as last element of list
    }

    public void addRootNode(int idx, DbNode node)
    {
        if (versionDbId != node.getVersionDbId()) {
            throw new RuntimeException("Node is assigned to different version!");
        }
        clearPersistedOrphans();
        
        // Remove from current parent node
        // DbNode oldParent = node.getParentNode();
        // if (oldParent != null) {
        //     oldParent.removeChildNode(node);
        // }
        
        removeFromRemovedNodes(node);
        node.setParentNode(null);
        getRootNodes().add(idx, node);
        updateRootNodeIndex();
    }

    public boolean removeRootNode(DbNode node)
    {
        boolean removed = getRootNodes().remove(node);
        if (removed) {
            clearPersistedOrphans();
            updateRootNodeIndex();
            addToRemovedNodes(node);
        }
        return removed;
    }

    public DbNode createNode(long nodeNumber)
    {
        NodeNumberRegistry.registerNumber(this, nodeNumber);
        DbNode nd = new DbNode();
        nd.setNodeNumber(nodeNumber);
        nd.setVersion(this);
        nd.setParentNode(null);
        nd.setLastModNodeDbId(-1);
        return nd;
    }

    public DbNode createNode()
    {
        DbNode nd = new DbNode();
        initNode(nd);
        return nd;
    }

//    public DbReference createReference()
//    {
//        DbReference nd = new DbReference();
//        initNode(nd);
//        return nd;
//    }

    private void initNode(DbNode nd)
    {
        long new_num = NodeNumberRegistry.getNewNumber(this);
        nd.setNodeNumber(new_num);
        // nd.versionDbId = versionDbId;
        nd.setVersion(this);
        nd.setParentNode(null);
        nd.setLastModNodeDbId(-1);
    }

    List getAliases() 
    {
        return aliases;
    }

    void setAliases(List aliases) 
    {
        this.aliases = aliases;
    }


    public String toString()
    {
        return "Version DB-ID: " + getVersionDbId() +
                "   Version-Name: " + getVersionName();
    }
    
    public void deleteOrphans(Session sess)
    {
        deleteOrphanAliases(sess);
        deleteOrphanNodes(sess);
    }
    
    public void deleteOrphanAliases(Session sess)
    {
        if ((removedAliases != null) && !removedAliases.isEmpty()) {
            boolean is_read_only = sess.isDefaultReadOnly();
            Iterator<DbAlias> it = removedAliases.iterator();
            while (it.hasNext()) {
                DbAlias dba = it.next();
                try {
                    if (sess.contains(dba)) {  // dba is not transient
                        if (is_read_only) {
                            sess.setReadOnly(dba, false);
                        }
                        sess.delete(dba);
                    }
                    it.remove();  // remove from list if object has been successfully deleted
                } catch (Exception ex) {
                    Log.error("Exception in deleteOrphanAliases: " + ex.getMessage());
                }
            }
        }
    }
    
    public void deleteOrphanNodes(Session sess)
    {
        if ((removedNodes != null) && !removedNodes.isEmpty()) {
            boolean is_read_only = sess.isDefaultReadOnly();
            Iterator<DbNode> it = removedNodes.iterator();
            while (it.hasNext()) {
                DbNode nd = it.next();
                try {
                    if (nd.getParentNode() == null) {  // assure that it is really an orphan
                        if (sess.contains(nd)) {  // nd is not transient
                            if (is_read_only) {
                                sess.setReadOnly(nd, false);
                            }
                            sess.delete(nd);
                        }
                    } else {
                        if (DocConstants.DEBUG) {
                            Log.warning("Skipping deletion of node with non-null parent: " + nd.getNodeDbId());
                        }
                    }
                    it.remove();  // remove from list if object has been successfully deleted
                } catch (Exception ex) {
                    Log.error("Exception in deleteOrphanAliases: " + ex.getMessage());
                }
            }
        }
    }

//    public boolean equals(Object obj)
//    {
//        if ((obj == null) || !(obj instanceof DbVersion)) {
//            return false;
//        }
//        final DbVersion other = (DbVersion) obj;
//        final String selfname = this.getVersionName();
//        final String othername = other.getVersionName();
//        if ((selfname == null) && (othername != null)) {
//            return false;
//        }
//        if ((selfname != null) && !selfname.equals(othername)) {
//            return false;
//        }
//        final DbStore selfstore = this.getStore();
//        final DbStore otherstore = other.getStore();
//        if ((selfstore == null) && (otherstore != null)) {
//            return false;
//        }
//        if ((selfstore != null) && !selfstore.equals(otherstore)) {
//            return false;
//        }
//        return true;
//    }
//
//
//
//    public int hashCode()
//    {
//        final String selfname = this.getVersionName();
//        final DbStore selfstore = this.getStore();
//        int hash = (selfname != null) ? selfname.hashCode() : 0;
//        hash += (selfstore != null) ? selfstore.hashCode() : 0;
//        return hash;
//    }


    private void clearPersistedOrphans()
    {
        Iterator it = rootNodes.iterator();
        while (it.hasNext()) {
            DbNode nd = (DbNode) it.next();
            if (nd.getChildPos() < 0) {
                addToRemovedNodes(nd);
                it.remove();
            }
        }
    }
    
    private void updateRootNodeIndex()
    {
        for (int i=0; i < rootNodes.size(); i++) {
            DbNode nd = (DbNode) rootNodes.get(i);
            nd.setChildPos(i);
        }
    }

    void addToRemovedAliases(DbAlias dba) 
    {
        if (dba == null) {
            throw new RuntimeException("DbAlias is null.");
        }
        if (removedAliases == null) {
            removedAliases = new ArrayList<DbAlias>();
        }
        // int removedDbId = dba.getVersion().getVersionDbId();
        // if (lastRemovedVerDbId != removedDbId) {
        //     removedAliases.clear();
        //     lastRemovedVerDbId = removedDbId;
        // }
        
        dba.setNode(null);
        removedAliases.add(dba);
    }
    
    DbAlias getFromRemovedAliases(String aliasName)
    {
        if (removedAliases == null) {
            return null;
        }
        int idx = calcAliasIndex(removedAliases, aliasName, 0);
        return (idx >= 0) ? removedAliases.remove(idx) : null;
    }

    private static int calcAliasIndex(List<DbAlias> alist, String aliasName, int startPos)
    {
        for (int i = startPos; i < alist.size(); i++) {
            if (aliasName.equals(alist.get(i).getAlias())) {
                return i;
            }
        }
        return -1;
    }
    
    void addToRemovedNodes(DbNode node) 
    {
        if (node == null) {
            throw new RuntimeException("DbNode is null.");
        }
        if (removedNodes == null) {
            removedNodes = new ArrayList<DbNode>();
        }
        // int removedDbId = node.getVersion().getVersionDbId();
        // if (lastRemovedVerDbId != removedDbId) {
        //     removedAliases.clear();
        //     lastRemovedVerDbId = removedDbId;
        // }
        
        node.setChildPos(-1);
        // node.setParentNode(null);
        removedNodes.add(node);
    }
    
    void removeFromRemovedNodes(DbNode node)
    {
        if (removedNodes == null) {
            return;
        }
        int idx = calcNodeIndex(removedNodes, node, 0);
        if (idx >= 0) { 
            removedNodes.remove(idx);
        }
    }


    private static int calcNodeIndex(List<DbNode> nlist, DbNode node, int startPos)
    {
        // boolean is_persisted = node.persisted();
        for (int i = startPos; i < nlist.size(); i++) {
            // if (is_persisted) {
            //     if (node.getNodeDbId() == nlist.get(i).getNodeDbId()) {
            //         return i;
            //     }
            // } else {
            if (node == nlist.get(i)) {   // same instance
                return i;
            }
            // }
        }
        return -1;
    }
}
