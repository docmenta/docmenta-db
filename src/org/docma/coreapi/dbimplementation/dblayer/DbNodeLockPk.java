/*
 * DbNodeLockPk.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 * This class is no longer used.
 * 
 * @author MP
 */
public class DbNodeLockPk implements java.io.Serializable
{
    private DbNode lockedNode;
    private String lockName;

    DbNodeLockPk()
    {
    }

    public String getLockName()
    {
        return lockName;
    }

    public void setLockName(String lockName)
    {
        this.lockName = lockName;
    }

    public DbNode getLockedNode()
    {
        return lockedNode;
    }

    void setLockedNode(DbNode node)
    {
        this.lockedNode = node;
    }


    public boolean equals(Object obj)
    {
        if (! (obj instanceof DbNodeLock)) {
            return false;
        }
        final DbNodeLockPk other = (DbNodeLockPk) obj;
        DbNode otherNode = other.getLockedNode();
        if (lockedNode != otherNode) {
            return false;
        }
        if ((lockedNode != null) && (lockedNode.getNodeDbId() != otherNode.getNodeDbId())) {
            return false;
        }
        if ((this.lockName == null) ? (other.lockName != null) : !this.lockName.equals(other.lockName)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        int hash;
        if (lockedNode == null) {
            hash = 1;
        } else {
            long nodeid = lockedNode.getNodeDbId();
            hash = (int) (nodeid ^ (nodeid >>> 32));
        }
        hash = 17 * hash + (this.lockName != null ? this.lockName.hashCode() : 0);
        return hash;
    }


}
