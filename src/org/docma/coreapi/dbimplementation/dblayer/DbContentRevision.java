/*
 * DbContentRevision.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class DbContentRevision 
{
    private long revisionDbId = 0;   // generated value; 0 means transient
    private int versionDbId;
    private long nodeNumber;
    private String langCode;
    private long timestamp;
    private String userId;
    private List lobWrapper = null;  // private Blob content = null;


    public DbContentRevision()
    {
    }
    
    public DbContentRevision(int verDbId, long nodeNumber, String langCode, long timestamp, String user)
    {
        this.versionDbId = verDbId;
        this.nodeNumber = nodeNumber;
        this.langCode = langCode;
        this.timestamp = timestamp;
        this.userId = user;
    }
    
    public long getRevisionDbId() 
    {
        return revisionDbId;
    }

    void setRevisionDbId(long revisionDbId) 
    {
        this.revisionDbId = revisionDbId;
    }

    public int getVersionDbId() 
    {
        return versionDbId;
    }

    void setVersionDbId(int versionDbId) 
    {
        this.versionDbId = versionDbId;
    }

    public long getNodeNumber() 
    {
        return nodeNumber;
    }

    void setNodeNumber(long nodeNumber) 
    {
        this.nodeNumber = nodeNumber;
    }

    public String getLangCode() 
    {
        return langCode;
    }

    void setLangCode(String langCode) 
    {
        this.langCode = langCode;
    }

    public long getTimestamp() 
    {
        return timestamp;
    }

    void setTimestamp(long timestamp) 
    {
        this.timestamp = timestamp;
    }

    public String getUserId() 
    {
        return userId;
    }

    void setUserId(String user) 
    {
        this.userId = user;
    }

    List getLobWrapper()
    {
        return this.lobWrapper;
    }

    void setLobWrapper(List lob_list)
    {
        this.lobWrapper = lob_list;
    }


    private void set_lob(DbRevisionLob lob)
    {
        lob.setOwner(this);
        List wrapper = getLobWrapper();
        if (wrapper == null) {
            wrapper = new ArrayList();
            setLobWrapper(wrapper);
        } else {
            wrapper.clear();
        }
        wrapper.add(lob);
    }
    
    private DbRevisionLob get_lob()
    {
        List wrapper = getLobWrapper();
        if ((wrapper == null) || wrapper.isEmpty()) {
            return null;
        }
        return (DbRevisionLob) wrapper.get(0);
    }
    

    public byte[] getContent() 
    {
        DbRevisionLob lobObj = get_lob();
        return (lobObj == null) ? new byte[0] : lobObj.getContent();
    }

    public void setContent(byte[] content) 
    {
        DbRevisionLob lobObj = get_lob();
        // writeDebugInfo(lobObj, null);
        if (lobObj == null) {
            lobObj = new DbRevisionLob();
            set_lob(lobObj);
        }
        lobObj.setContent(content);
    }

    
}
