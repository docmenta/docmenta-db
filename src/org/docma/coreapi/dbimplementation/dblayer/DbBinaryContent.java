/*
 * DbBinaryContent.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.List;
import java.util.ArrayList;
import java.sql.Blob;
import org.docma.coreapi.DocConstants;
import org.hibernate.Session;
import org.docma.coreapi.dbimplementation.DocNodeContext;
import org.docma.util.Log;

/**
 *
 * @author MP
 */
public class DbBinaryContent implements DbNodeEntity
{
    private long binaryDbId = 0;   // generated value; 0 means transient
    private DbNode owner;
    private String langCode;
    private List lobWrapper = null;
    private long contentLength = 0;
    private String contentType = "";
    
    private long lobTransactionCounter = -1;
    

    DbBinaryContent()
    {
        // set_lob_wrapper(new DbBinaryLob());
    }


    private void set_lob(DbBinaryLob lob)
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
    
    private DbBinaryLob get_lob()
    {
        List wrapper = getLobWrapper();
        if ((wrapper == null) || wrapper.isEmpty()) {
            return null;
        }
        return (DbBinaryLob) wrapper.get(0);
    }

    public long getBinaryDbId()
    {
        return binaryDbId;
    }

    void setBinaryDbId(long binaryDbId)
    {
        this.binaryDbId = binaryDbId;
    }

    public boolean persisted()
    {
        return binaryDbId > 0;
    }

    List getLobWrapper()
    {
        return this.lobWrapper;
    }

    void setLobWrapper(List lob_list)
    {
        this.lobWrapper = lob_list;
    }

    public Blob getContent()
    {
        DbBinaryLob lobObj = get_lob();
        writeDebugInfo(lobObj, null);
        return (lobObj == null) ? null : lobObj.getContent();
    }
    
    public Blob getNodeContent(DocNodeContext ctx)
    {
        DbBinaryLob lobObj = prepareLobAccess(ctx);
        writeDebugInfo(lobObj, ctx);
        return (lobObj == null) ? null : lobObj.getContent();
    }

    void setContent(Blob content)
    {
        DbBinaryLob lobObj = get_lob();
        writeDebugInfo(lobObj, null);
        if (lobObj == null) {
            lobObj = new DbBinaryLob();
            set_lob(lobObj);
        }
        lobObj.setContent(content);
    }
    
    public void setNodeContent(Blob content, DocNodeContext ctx)
    {
        DbBinaryLob lobObj = get_lob();
        writeDebugInfo(lobObj, ctx);
        if (lobObj == null) {
            lobObj = new DbBinaryLob();
            set_lob(lobObj);
        } else {
            if (lobObj.persisted()) {
                ctx.getDbSession().setReadOnly(lobObj, false); // allow update
            }
        }
        lobObj.setContent(content);
        
        // Note that some JDBC drivers (e.g. Oracle) do not allow to read
        // from the same Blob object that was used to write the content.
        // As a workaround the object containing the Blob needs to be refreshed
        // before reading from the Blob. See method prepareLobAccess().
        this.lobTransactionCounter = ctx.getDbTransactionCounter();
    }

    public long getContentLength()
    {
        return contentLength;
    }

    public void setContentLength(long contentLength)
    {
        this.contentLength = contentLength;
    }


    public String getContentType()
    {
        return contentType;
    }

    public void setContentType(String contentType)
    {
        this.contentType = contentType;
    }


    public String getLangCode()
    {
        return langCode;
    }

    public void setLangCode(String langCode)
    {
        this.langCode = langCode;
    }


    public DbNode getOwner()
    {
        return owner;
    }

    void setOwner(DbNode owner)
    {
        this.owner = owner;
    }


    public void refreshContent(Session sess)
    {
        DbBinaryLob lobObj = get_lob();
        if (DocConstants.DEBUG) {
            Log.info("refreshContent(). DbBinaryContent DB Id: " + this.getBinaryDbId() +
                     ", DbBinaryLob: " + ((lobObj != null) ? lobObj.getLobDbId() : "null"));
        }
        if ((lobObj != null) && lobObj.persisted()) { 
            sess.refresh(lobObj);
        }
    }
    
    private DbBinaryLob prepareLobAccess(DocNodeContext ctx) 
    {
        DbBinaryLob lobObj = get_lob();
        if (lobObj != null) {
            long currentCounter = ctx.getDbTransactionCounter();
            if (lobObj.persisted()) {
                if (this.lobTransactionCounter >= 0) {  // Blob has already been accessed
                    // if (currentCounter != this.lobTransactionCounter) { // Blob has been accessed in other transaction.
                        // Blob has to be reloaded, because Blob object
                        // is only valid within the same transaction.
                        // Furthermore some JDBC drivers (e.g. Oracle) do not
                        // allow reading from and/or writing to the same Blob
                        // object multiple times within the same transaction.
                        // Workaround is to refresh the object containing the Blob.
                        ctx.getDbSession().refresh(lobObj);
                    // }
                }
            } else {
                Session dbsess = ctx.getDbSession();
                // System.out.println("BLOB id before flush: " + lobObj.getLobDbId());
                dbsess.flush();           // persist blob
                // System.out.println("BLOB id before refresh: " + lobObj.getLobDbId());
                dbsess.refresh(lobObj);   // get persisted blob
                // System.out.println("BLOB id after refresh: " + lobObj.getLobDbId());
            }
            this.lobTransactionCounter = currentCounter;
        }
        return lobObj;
    }
    
    private void writeDebugInfo(DbBinaryLob lobObj, DocNodeContext ctx) 
    {
        if (DocConstants.DEBUG) {
            System.out.println("BLOB access: getNodeContent(). Binary Id: " + binaryDbId +
              ". BLOB id: " + ((lobObj == null) ? "null" : lobObj.getLobDbId()) + 
              ". Owner: " + ((owner == null) ? "null" : (owner.getNodeNumber() + "/" + owner.getNodeDbId())) +
              ". Version: " + (((owner != null) && (owner.getVersion() != null)) ? owner.getVersion().getVersionName() : "") +
              ". Last Transaction: " + this.lobTransactionCounter +
              ((ctx == null) ? "" : ". Current Transaction: " + ctx.getDbTransactionCounter()));
        }
    }
}
