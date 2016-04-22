/*
 * DbNodeLock.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbNodeLock
{
    private long lockDbId = 0;  // generated value; 0 means transient
    // private DbNodeLockPk id = new DbNodeLockPk();
    private DbNode lockedNode;
    private String lockName;
    private String userId;
    private long timeout;
    private long creationTime;


    DbNodeLock()
    {
    }

    public long getLockDbId() 
    {
        return lockDbId;
    }

    void setLockDbId(long lockDbId) 
    {
        this.lockDbId = lockDbId;
    }


    public long getCreationTime()
    {
        return creationTime;
    }

    public void setCreationTime(long creationTime)
    {
        this.creationTime = creationTime;
    }


//    DbNodeLockPk getId()
//    {
//        return id;
//    }
//
//    void setId(DbNodeLockPk id)
//    {
//        this.id = id;
//    }


    public DbNode getLockedNode()
    {
        return lockedNode; // id.getLockedNode();
    }

    void setLockedNode(DbNode lockedNode)
    {
        this.lockedNode = lockedNode; // id.setLockedNode(lockedNode);
        // this.id.setNodeDbId(lockedNode.getNodeDbId());
    }


    public String getLockName()
    {
        return lockName; // id.getLockName();
    }

    public void setLockName(String lockName)
    {
        this.lockName = lockName; // this.id.setLockName(lockName);
    }


    public long getTimeout()
    {
        return timeout;
    }

    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }


    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }



//    public boolean equals(Object obj)
//    {
//        if (! (obj instanceof DbNodeLock)) {
//            return false;
//        }
//        final DbNodeLock other = (DbNodeLock) obj;
//        if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
//            return false;
//        }
//        return true;
//    }
//
//
//    public int hashCode()
//    {
//        return (this.id != null) ? this.id.hashCode() : 0;
//    }

    @Override
    public int hashCode() 
    {
        return (int) (this.lockDbId ^ (this.lockDbId >>> 32));
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DbNodeLock other = (DbNodeLock) obj;
        if (this.lockDbId != other.lockDbId) {
            return false;
        }
        // Note that it is not sufficient to just compare the lockDbId, because
        // the lockDbId is not set as long as the object has not been persisted.
        if ((this.lockName == null) ? (other.lockName != null) : !this.lockName.equals(other.lockName)) {
            return false;
        }
        if ((this.userId == null) ? (other.userId != null) : !this.userId.equals(other.userId)) {
            return false;
        }
        if (this.creationTime != other.creationTime) {
            return false;
        }
        return true;
    }

}
