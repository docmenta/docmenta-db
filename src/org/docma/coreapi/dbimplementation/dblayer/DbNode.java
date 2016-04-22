/*
 * DbNode.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class DbNode implements DbNodeEntity
{
    private long nodeDbId = 0;   // generated value; 0 means transient
    private long nodeNumber;
    private String nodeType;
    private int childPos = 0;

    private int versionDbId;
    private DbVersion version = null;
    private DbNode parentNode = null;
    private List childNodes = new ArrayList();
    private Map attributes = new HashMap();   // Map<NodeAttributeKey, String>
    private List aliasList = new ArrayList();
    private Set locks = new HashSet();
    private Map binaryContent = new HashMap();  // Map<String, DbBinaryContent>, String = language code
    private Map textContent = new HashMap();  // Map<String, DbTextContent>, String = language code
    private Map imageRenditions = new HashMap();  // Map<ImageRenditionKey, DbImageRendition>

    Long lastModNodeDbId = null; // private long lastModNodeDbId = -1;
    DbNode lastModifiedNode = null;   // only required for join fetch in DocStoreDbConnection.listNodeInfos()

    DbNode()
    {
    }


    public long getNodeDbId()
    {
        return nodeDbId;
    }

    void setNodeDbId(long nodeDbId)
    {
        this.nodeDbId = nodeDbId;
    }
    
    public boolean persisted()
    {
        return getNodeDbId() > 0;
    }

    public long getNodeNumber()
    {
        return nodeNumber;
    }

    public void setNodeNumber(long nodeNumber)
    {
        this.nodeNumber = nodeNumber;
    }


    int getChildPos()
    {
        return childPos;
    }

    void setChildPos(int childpos)
    {
        this.childPos = childpos;
    }


    public String getNodeType()
    {
        return nodeType;
    }

    public void setNodeType(String nodeType)
    {
        this.nodeType = nodeType;
    }


    List getChildNodes()
    {
        return childNodes;
    }

    void setChildNodes(List childNodes)
    {
        this.childNodes = childNodes;
    }

    public List allChildNodes()
    {
        return Collections.unmodifiableList(this.childNodes);
    }

    public void addChildNode(DbNode node)
    {
        addChildNode(getChildNodes().size(), node);  // add as last element of list
    }

    public void addChildNode(int idx, DbNode node)
    {
        if (this.getVersionDbId() != node.getVersionDbId()) {
            throw new RuntimeException("Node is assigned to different version!");
        }
        // Remove from current parent node
        DbNode oldParent = node.getParentNode();
        if (oldParent != null) {
            oldParent.removeChildNode(node);
        }
        
        // Set this node as new parent
        getVersion().removeFromRemovedNodes(node);
        node.setParentNode(this);
        getChildNodes().add(idx, node);
        updateChildIndex();
    }

    public boolean removeChildNode(DbNode node)
    {
        boolean removed = getChildNodes().remove(node);
        if (removed) {
            node.setParentNode(null);
            updateChildIndex();
            getVersion().addToRemovedNodes(node);
        }
        return removed;
    }

    public DbNode removeChildNode(int idx)
    {
        DbNode removed = (DbNode) getChildNodes().remove(idx);
        if (removed != null) {
            removed.setParentNode(null);
            updateChildIndex();
            getVersion().addToRemovedNodes(removed);
        }
        return removed;
    }


    public DbNode getParentNode()
    {
        return parentNode;
    }

    void setParentNode(DbNode parentNode)
    {
        this.parentNode = parentNode;
    }

    int getVersionDbId()
    {
        return versionDbId;
    }
    
    void setVersionDbId(int ver_db_id) 
    {
        this.versionDbId = ver_db_id;
    }

    public DbVersion getVersion()
    {
        return version;
    }

    void setVersion(DbVersion version)
    {
        this.version = version;
        setVersionDbId(version.getVersionDbId());
    }


    public Map getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Map attributes)
    {
        this.attributes = attributes;
    }

    public String getAttribute(String lang_code, String name)
    {
        return (String) getAttributes().get(new NodeAttributeKey(lang_code, name));
    }

    public void setAttribute(String lang_code, String name, String value)
    {
        getAttributes().put(new NodeAttributeKey(lang_code, name), value);
    }

    public String removeAttribute(String lang_code, String name)
    {
        return (String) getAttributes().remove(new NodeAttributeKey(lang_code, name));
    }

    public int removeAttributes(String lang_code, String name)
    {
        Iterator it = getAttributes().keySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            NodeAttributeKey attkey = (NodeAttributeKey) it.next();
            if (((lang_code == null) || lang_code.equals(attkey.getLangCode())) &&
                ((name == null) || name.equals(attkey.getAttName()))) {
                it.remove();
                ++count;
            }
        }
        return count;
    }


    List getAliasList()
    {
        return aliasList;
    }

    void setAliasList(List aliases)
    {
        this.aliasList = aliases;
    }


    public String getAlias(int pos)
    {
        List alist = getAliasList();
        if (alist == null) {
            return null;
        }
        return ((DbAlias) alist.get(pos)).getAlias();
    }
    
    public String[] getAliases()
    {
        List alist = getAliasList();
        if (alist == null) {
            return null;
        }
        String[] res = new String[alist.size()];
        for (int i=0; i < res.length; i++) {
            res[i] = ((DbAlias) alist.get(i)).getAlias();
        }
        return res;
    }

    public void setAliases(String[] aliases)
    {
        List alist = getAliasList();
        boolean is_new_list = false;
        if (alist == null) {
            alist = new ArrayList();
            is_new_list = true;
        }
        if (aliases == null) {
            alist.clear();
        } else {
            DbVersion ver = getVersion();
            for (int i=0; i < aliases.length; i++) {
                int current_pos = calcAliasIndex(alist, aliases[i], i);
                if (current_pos < 0) {  // alias does not exist yet
                    DbAlias dba = ver.getFromRemovedAliases(aliases[i]);
                    if (dba == null) {
                        dba = new DbAlias();
                        dba.setAlias(aliases[i]);
                    }
                    dba.setNode(this);
                    dba.setVersion(ver);
                    // dba.setPos(i);
                    alist.add(i, dba);
                } else
                if (current_pos > i) {  // alias already exists at other position
                    DbAlias dba = (DbAlias) alist.remove(current_pos);
                    alist.add(i, dba);  // move alias to new position
                } 
                // else if (current_pos == i) {
                //    // alias already exists at same position, i.e. nothing to do
                // }
            }
            
            // Remove aliases that are not in the new list of aliases
            if (alist.size() > aliases.length) {
                for (int i = alist.size() - 1; i >= aliases.length; i--) {
                    ver.addToRemovedAliases((DbAlias) alist.remove(i));
                }
            }
        }
        if (is_new_list) {
            setAliasList(alist);
        }
    }
    
    public void appendAlias(String alias)
    {
        insertAlias(aliasCount(), alias);
    }
    
    public void insertAlias(int pos, String alias)
    {
        if (pos < 0) {
            throw new RuntimeException("Invalid alias index: " + pos);
        }
        DbVersion ver = getVersion();
        DbAlias dba = ver.getFromRemovedAliases(alias);
        if (dba == null) {
            dba = new DbAlias();
            dba.setAlias(alias);
        }
        dba.setNode(this);
        dba.setVersion(ver);
        // dba.setPos(pos);
        
        List alist = getAliasList();
        boolean is_new_list = false;
        if (alist == null) {
            alist = new ArrayList();
            is_new_list = true;
        }
        if (pos > alist.size()) {
            pos = alist.size();
        }
        alist.add(pos, dba);
        if (is_new_list) {
            setAliasList(alist);
        }
    }
    
    public void replaceAlias(int pos, String alias) 
    {
        deleteAlias(pos);
        insertAlias(pos, alias);
        // DbAlias dba = (DbAlias) getAliasList().get(pos);
        // dba.setAlias(alias);
    }
    
    public void deleteAlias(int pos)
    {
        DbAlias dba = (DbAlias) getAliasList().remove(pos);
        getVersion().addToRemovedAliases(dba);
    }
    
    public boolean deleteAlias(String alias)
    {
        List alist = getAliasList();
        if (alist == null) {
            return false;
        } else {
            int pos = calcAliasIndex(alist, alias, 0);
            if (pos < 0) {
                return false;
            } else {
                deleteAlias(pos);
                return true;
            }
        }
    }
    
    public int aliasIndex(String alias) 
    {
        List alist = getAliasList();
        return (alist == null) ? -1 : calcAliasIndex(alist, alias, 0);
    }
    
    public int aliasCount()
    {
        List alist = getAliasList();
        return (alist == null) ? 0 : alist.size();
    }


    public Set getLocks()
    {
        return locks;
    }

    void setLocks(Set locks)
    {
        this.locks = locks;
    }

    public DbNodeLock addLock(String lockName, String userId, long creationTime, long timeout)
    {
        DbNodeLock lock = new DbNodeLock();
        lock.setLockedNode(this);
        lock.setLockName(lockName);
        lock.setCreationTime(creationTime);
        lock.setTimeout(timeout);
        lock.setUserId(userId);
        getLocks().add(lock);
        return lock;
    }

    public DbNodeLock getLock(String lockName)
    {
        Iterator it = getLocks().iterator();
        while (it.hasNext()) {
            DbNodeLock lock = (DbNodeLock) it.next();
            if (lockName.equals(lock.getLockName())) {
                return lock;
            }
        }
        return null;
    }

    public boolean removeLock(String lockName)
    {
        Iterator it = getLocks().iterator();
        while (it.hasNext()) {
            DbNodeLock lock = (DbNodeLock) it.next();
            if (lockName.equals(lock.getLockName())) {
                it.remove();
                return true;
            }
        }
        return false;
    }


    Map getBinaryContent()
    {
        return binaryContent;
    }

    void setBinaryContent(Map binaryContent)
    {
        this.binaryContent = binaryContent;
    }

    public Map allBinaryContent()
    {
        return Collections.unmodifiableMap(this.binaryContent);
    }

    public DbBinaryContent getBinaryContent(String language)
    {
        return (DbBinaryContent) getBinaryContent().get(language);
    }

    public DbBinaryContent createBinaryContent(String language)
    {
        DbBinaryContent cont = new DbBinaryContent();
        cont.setOwner(this);
        cont.setLangCode(language);
        getBinaryContent().put(language, cont);
        return cont;
    }

    public DbBinaryContent removeBinaryContent(String language)
    {
        DbBinaryContent cont = (DbBinaryContent) getBinaryContent().remove(language);
        // if (cont != null) {
        //     cont.setOwner(null);
        // }
        return cont;
    }


    Map getTextContent()
    {
        return textContent;
    }

    void setTextContent(Map textContent)
    {
        this.textContent = textContent;
    }

    public Map allTextContent()
    {
        return Collections.unmodifiableMap(this.textContent);
    }

    public DbTextContent getTextContent(String language)
    {
        return (DbTextContent) getTextContent().get(language);
    }

    public DbTextContent createTextContent(String language)
    {
        DbTextContent cont = new DbTextContent();
        cont.setOwner(this);
        cont.setLangCode(language);
        getTextContent().put(language, cont);
        return cont;
    }

    public DbTextContent removeTextContent(String language)
    {
        DbTextContent cont = (DbTextContent) getTextContent().remove(language);
        // if (cont != null) {
        //     cont.setOwner(null);
        // }
        return cont;
    }


    Map getImageRenditions()
    {
        return imageRenditions;
    }

    void setImageRenditions(Map imageRenditions)
    {
        this.imageRenditions = imageRenditions;
    }

    public Map allImageRenditions()
    {
        return Collections.unmodifiableMap(this.imageRenditions);
    }

    public DbImageRendition getImageRendition(String language, String renditionName)
    {
        return (DbImageRendition) getImageRenditions().get(
                  new ImageRenditionKey(language, renditionName));
    }

    public DbImageRendition createImageRendition(String language, String renditionName)
    {
        DbImageRendition rend = new DbImageRendition();
        rend.setOwner(this);
        rend.setLangCode(language);
        rend.setRenditionName(renditionName);
        getImageRenditions().put(new ImageRenditionKey(language, renditionName), rend);
        return rend;
    }

    public DbImageRendition removeImageRendition(String language, String renditionName)
    {
        return removeImageRendition(new ImageRenditionKey(language, renditionName));
    }

    public DbImageRendition removeImageRendition(ImageRenditionKey renditionKey)
    {
        DbImageRendition rend = (DbImageRendition) getImageRenditions().remove(renditionKey);
        // if (rend != null) {
        //     rend.setOwner(null);
        // }
        return rend;
    }

    public int removeImageRenditions(String language, String renditionName)
    {
        Iterator it = getImageRenditions().entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            ImageRenditionKey key = (ImageRenditionKey) entry.getKey();
            if (((language == null) || language.equals(key.getLangCode())) &&
                ((renditionName == null) || renditionName.equals(key.getRenditionName()))) {
                DbImageRendition rend = (DbImageRendition) entry.getValue();
                it.remove();
                // rend.setOwner(null);
                ++count;
            }
        }
        return count;
    }


//    public long getLastModNodeDbId()
//    {
//        return lastModNodeDbId;
//    }
//
//    public void setLastModNodeDbId(long lastModNodeDbId)
//    {
//        this.lastModNodeDbId = lastModNodeDbId;
//    }

    public long getLastModNodeDbId()
    {
        if (lastModNodeDbId == null) return -1;
        return lastModNodeDbId.longValue();
    }

    public void setLastModNodeDbId(long modNodeDbId)
    {
        this.lastModNodeDbId = (modNodeDbId < 0) ? null : modNodeDbId;
    }

//    public DbNode getLastModifiedNode() 
//    {
//        return lastModifiedNode;
//    }
//
//    /**
//     * Setting the last modified node should be done via setLastModNodeDBId.
//     * 
//     * @param lastModifiedNode 
//     */
//    void setLastModifiedNode(DbNode lastModifiedNode) 
//    {
//        if (lastModifiedNode == null) {
//            this.lastModNodeDbId = null;
//        } else {
//            if (lastModifiedNode.persisted()) {
//                this.lastModNodeDbId = lastModifiedNode.getNodeDbId();
//            } else {
//                throw new RuntimeException("Cannot set transient node as last modified node!");
//            }
//        }
//        this.lastModifiedNode = lastModifiedNode;
//    }


    public void removeTranslation(String lang)
    {
        if ((lang == null) || (lang.length() == 0)) {
            throw new RuntimeException("Error in removeTranslation(). Language code is null or empty string.");
        }
        removeAttributes(lang, null);  // delete all translated attributes
        removeBinaryContent(lang);     // delete translated binary content
        removeTextContent(lang);       // delete translated text content
        removeImageRenditions(lang, null);  // delete image renditions
    }


    public String toString()
    {
        return "Node(DB-ID: " + getNodeDbId() + "  Node-Number: " + getNodeNumber() +
               "  Version-ID: " + getVersionDbId() +
               ")";
    }


    public boolean equals(Object obj)
    {
        if (! (obj instanceof DbNode)) {
            return false;
        }
        final DbNode other = (DbNode) obj;
        if (this.getNodeNumber() != other.getNodeNumber()) {
            return false;
        }
        if (this.getVersionDbId() != other.getVersionDbId()) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        long nn = this.getNodeNumber();
        return (int) (nn ^ (nn >>> 32));
    }


    private void updateChildIndex()
    {
        for (int i=0; i < childNodes.size(); i++) {
            DbNode nd = (DbNode) childNodes.get(i);
            nd.setChildPos(i);
        }
    }
    
    private int calcAliasIndex(List alist, String aliasName, int startPos)
    {
        for (int i = startPos; i < alist.size(); i++) {
            if (aliasName.equals(((DbAlias) alist.get(i)).getAlias())) {
                return i;
            }
        }
        return -1;
    }
    
}
