/*
 * DbPublicationExport.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;
import java.sql.Blob;
import org.hibernate.Session;

import org.docma.util.Log;

/**
 *
 * @author MP
 */
public class DbPublicationExport
{
    private int pubExportDbId = 0;   // primary key; generated value; 0 means transient
    private String exportName = null;
    private List lobWrapper = null;   // private java.sql.Blob exportFile = null;
    private long exportSize = 0;
    private java.sql.Blob exportLog = null;
    private Boolean has_log = null;

    private DbVersion version;   // foreign key: versionDbId
    private Map<String, String> attributes = new HashMap<String, String>();  // private Set attributes = new LinkedHashSet();

    // private int exportLobAccessCounter = 0;
    // private int logAccessCounter = 0;

    public DbPublicationExport()
    {
    }


    private void set_lob(DbPubExportLob lob)
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
    
    private DbPubExportLob get_lob()
    {
        List wrapper = getLobWrapper();
        if ((wrapper == null) || wrapper.isEmpty()) {
            return null;
        }
        return (DbPubExportLob) wrapper.get(0);
    }


    public String getExportName()
    {
        return exportName;
    }

    public void setExportName(String exportName)
    {
        this.exportName = exportName;
    }


    List getLobWrapper()
    {
        return this.lobWrapper;
    }

    void setLobWrapper(List lob_list)
    {
        this.lobWrapper = lob_list;
    }


    public Blob getExportFile()
    {
        // if (++exportLobAccessCounter > 1) {
        //     Log.warning("Publication export BLOB accessed multiple times: " + exportLobAccessCounter);
        // }
        DbPubExportLob lobObj = get_lob();
        return (lobObj == null) ? null : lobObj.getContent();
    }
    
    public void refreshExportFileBlob(Session dbSess) 
    {
        DbPubExportLob lobObj = get_lob();
        if (lobObj != null) {
            // System.out.println("Refreshing lob object: " + lobObj.getLobDbId());
            dbSess.refresh(lobObj);
            // exportLobAccessCounter = 0;
        }
    }
    
    public void saveExportFileBlob(Session dbSess) 
    {
        DbPubExportLob lobObj = get_lob();
        if (lobObj != null) {
            // System.out.println("Saving lob object: " + lobObj.getLobDbId());
            // dbSess.saveOrUpdate(lobObj);
            if (lobObj.persisted()) {
                dbSess.update(lobObj);
            } else {
                dbSess.save(lobObj);
            }
            // exportLobAccessCounter = 0;
        }
    }
    
//    public long getExportFileBlobSize()
//    {
//        DbPubExportLob lobObj = get_lob();
//        if (lobObj != null) {
//            Blob blob = lobObj.getContent();
//            try {
//                return (blob == null) ? 0 : blob.length();
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
//        } else {
//            return 0;
//        }
//    }

    public void setExportFile(Blob exportFile)
    {
        DbPubExportLob lobObj = get_lob();
        // writeDebugInfo(lobObj, null);
        if (lobObj == null) {
            lobObj = new DbPubExportLob();
            set_lob(lobObj);
        }
        lobObj.setContent(exportFile);
        
        // Note that some JDBC drivers (e.g. Oracle) do not allow to read
        // from the same Blob object that was used to write the content.
        // As a workaround the object containing the Blob needs to be refreshed
        // before reading from the Blob. See methods getExportFile() and 
        // refreshExportFileBlob().
        // exportLobAccessCounter = 1;
    }


    public Blob getExportLog()
    {
        // if (++logAccessCounter > 1) {
        //     Log.warning("Publication export log accessed multiple times: " + logAccessCounter + 
        //                 ", Db-Id: " + pubExportDbId + ", Name: " + exportName);
        // }
        return exportLog;
    }

    public void setExportLog(Blob exportLog)
    {
        this.exportLog = exportLog;
        // Note that some JDBC drivers (e.g. Oracle) do not allow to read
        // from the same Blob object that was used to write the content.
        // Therefore setting and getting the log within the same session
        // has to be avoided. See method getExportLog().
        // logAccessCounter = 1;
    }


    public boolean hasExportLog()
    {
        // Note: The result value is cached in the field has_log, to avoid
        //       accessing the blob multiple times (some JDBC drivers do not
        //       allow reading the blob multiple times).
        if (has_log == null) {
            Blob blob = getExportLog();
            try {
                has_log = (blob != null) && (blob.length() > 0);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return has_log.booleanValue();
    }

    public long getExportSize()
    {
        return exportSize;
    }

    public void setExportSize(long exportSize)
    {
        this.exportSize = exportSize;
    }


    public int getPubExportDbId()
    {
        return pubExportDbId;
    }

    void setPubExportDbId(int pubExportDbId)
    {
        this.pubExportDbId = pubExportDbId;
    }


    public DbVersion getVersion()
    {
        return version;
    }

    void setVersion(DbVersion version)
    {
        this.version = version;
    }


    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Map attributes)
    {
        this.attributes = attributes;
    }


    public String toString()
    {
        return "Publication Export ID: " + pubExportDbId + "  Size: " + exportSize;
    }



//    Set getAttributes() {
//        return attributes;
//    }
//
//    void setAttributes(Set attributes) {
//        this.attributes = attributes;
//    }
//
//    public Set allAttributes()
//    {
//        return Collections.unmodifiableSet(attributes);
//    }
//
//    public DbPubExportAttribute getAttribute(String name)
//    {
//        Iterator it = getAttributes().iterator();
//        while (it.hasNext()) {
//            DbPubExportAttribute att = (DbPubExportAttribute) it.next();
//            if (att.getAttName().equals(name)) return att;
//        }
//        return null;
//    }
//
//    public String getAttributeValue(String name)
//    {
//        DbPubExportAttribute att = getAttribute(name);
//        return (att != null) ? att.getAttValue() : null;
//    }
//
//    public void setAttribute(String name, String value)
//    {
//        DbPubExportAttribute att = getAttribute(name);
//        if (att == null) {
//            if (value == null) return;     // cannot remove non-existing attribute -> do nothing
//            att = new DbPubExportAttribute();
//            att.setPublicationExport(this); // att.getId().setPubExportDbId(pubExportDbId);
//            att.setAttName(name);           // att.getId().setAttName(name);
//            att.setAttValue(value);
//            getAttributes().add(att);
//        } else {
//            if (value == null) {
//                getAttributes().remove(att);
//                att.setPublicationExport(null);
//            } else {
//                att.setAttValue(value);
//            }
//        }
//    }

}
