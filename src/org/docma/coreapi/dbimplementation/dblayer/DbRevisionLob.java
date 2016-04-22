/*
 * DbRevisionLob.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbRevisionLob 
{
    private long lobDbId = 0;   // generated value; 0 means transient
    private DbContentRevision owner;
    private byte[] content = null;

    DbRevisionLob()
    {
    }
    
    public long getLobDbId() 
    {
        return lobDbId;
    }

    void setLobDbId(long lobDbId) 
    {
        this.lobDbId = lobDbId;
    }

    public boolean persisted()
    {
        return lobDbId > 0;
    }

    public DbContentRevision getOwner() 
    {
        return owner;
    }

    void setOwner(DbContentRevision owner) 
    {
        this.owner = owner;
    }

    public byte[] getContent()
    {
        return content;
    }

    public void setContent(byte[] content)
    {
        this.content = content;
    }


}
