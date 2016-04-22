/*
 * DbBinaryLob.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

import java.sql.Blob;

/**
 *
 * @author MP
 */
public class DbBinaryLob 
{
    private long lobDbId = 0;   // generated value; 0 means transient
    private DbBinaryContent owner;
    private java.sql.Blob content = null;

    DbBinaryLob()
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

    Blob getContent()
    {
        return content;
    }

    void setContent(Blob content)
    {
        this.content = content;
    }

    public DbBinaryContent getOwner() 
    {
        return owner;
    }

    void setOwner(DbBinaryContent owner) 
    {
        this.owner = owner;
    }


}
