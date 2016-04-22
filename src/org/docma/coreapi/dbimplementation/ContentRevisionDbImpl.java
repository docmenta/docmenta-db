/*
 * ContentRevisionDbImpl.java
 */
package org.docma.coreapi.dbimplementation;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.Date;
import java.sql.Blob;
import org.hibernate.Session;
import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.DbContentRevision;

/**
 *
 * @author MP
 */
public class ContentRevisionDbImpl implements DocContentRevision
{
    private final DocStoreDbConnection dbConn;  // required for lazy loading of content
    private long revisionDbId;
    private Date revisionDate;
    private String userId;
    private byte[] content = null;
    
    public ContentRevisionDbImpl(DocStoreDbConnection dbConn, long revDbId, Date revDate, String userId)
    {
        this.dbConn = dbConn;
        this.revisionDbId = revDbId;
        this.revisionDate = revDate;
        this.userId = userId;
    }

    public Date getDate() 
    {
        return revisionDate;
    }

    public String getUserId() 
    {
        return userId;
    }

    public byte[] getContent() 
    {
        loadContent();    // load content on request (lazy loading)
        return content;
    }

    public InputStream getContentStream() 
    {
        loadContent();    // load content on request (lazy loading)
        return new ByteArrayInputStream(content);
    }

    public String getContentString() 
    {
        return getContentString("UTF-8");
    }

    public String getContentString(String charsetName) 
    {
        loadContent();    // load content on request (lazy loading)
        try {
            return new String(content, charsetName);
        } catch (Exception ex) { 
            throw new DocRuntimeException(ex);
        }
    }

    public long getContentLength() 
    {
        loadContent();    // load content on request (lazy loading)
        return content.length;
    }
    
    private void loadContent()
    {
        if (content == null) {
            content = getRevisionContent();
        }
    }
    
    private byte[] getRevisionContent() 
    {
        synchronized (dbConn) {
            boolean started = dbConn.startLocalTransaction();
            try {
                byte[] res = null;
                Session dbSession = dbConn.getDbSession();
                DbContentRevision db_rev = (DbContentRevision) 
                  dbSession.get(DbContentRevision.class, new Long(revisionDbId));
                if (db_rev != null) {
                    res = db_rev.getContent();
                }
                dbConn.commitLocalTransaction(started);
                return (res == null) ? new byte[0] : res;
            } catch (Exception ex) {
                dbConn.rollbackLocalTransactionRuntime(started, ex);
                return null;
            }
        }
    }

}
