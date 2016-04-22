/*
 * PublicationArchiveDbImpl.java
 */

package org.docma.app.dbimplementation;

import java.io.*;
import java.util.*;
import java.sql.Blob;
import org.hibernate.*;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.VersionIdFactory;
import org.docma.util.Log;
import org.docma.util.DocmaUtil;
import org.docma.hibernate.*;
import org.docma.coreapi.dbimplementation.DbUtil;
import org.docma.coreapi.dbimplementation.dblayer.*;

/**
 *
 * @author MP
 */
public class PublicationArchiveDbImpl implements PublicationArchive
{
    private static final String TEMPFILE_PREFIX = "pubexp";
    private static final String PUB_PREFIX = "publication";
    private int  tempDeleteCounter = 0;

    private String docStoreId;
    private DocVersionId versionId;
    private VersionIdFactory versionIdFactory;
    private org.hibernate.SessionFactory dbFactory;
    private DbWork db;
    private boolean refreshFlag = false;
    private File tempDir;
    private Integer versionDbId = null;
    private Map outStreamMap = new Hashtable();  // use synchronized map

    public PublicationArchiveDbImpl(String docStoreId, 
                                    DocVersionId versionId,
                                    VersionIdFactory versionIdFactory,
                                    DbConnectionData dbConnection,
                                    File tempDir)
    {
        this(docStoreId, versionId, versionIdFactory, HibernateUtil.getSessionFactory(dbConnection), tempDir);
    }

    public PublicationArchiveDbImpl(String docStoreId, 
                                    DocVersionId versionId,
                                    VersionIdFactory versionIdFactory,
                                    org.hibernate.SessionFactory dbFactory,
                                    File tempDir)
    {
        this.docStoreId = docStoreId;
        this.versionId = versionId;
        this.versionIdFactory = versionIdFactory;
        this.dbFactory = dbFactory;
        this.db = new DbWork(dbFactory, 1000*60*2);  // Keep read cache for two minutes
        this.tempDir = tempDir;
        this.versionDbId = null;

        if (! tempDir.exists()) {
            if (! tempDir.mkdirs()) {
                Log.error("PublicationArchiveDbImpl: Could not create temporary directory: " +
                          tempDir.getAbsolutePath());
            }
        }
    }

    /* --------------  Interface PublicationArchive  --------------- */

    public String getDocStoreId()
    {
        return docStoreId;
    }

    public DocVersionId getVersionId()
    {
        return versionId;
    }

    public synchronized String createPublication(String language, String filename)
    {
        debug("-- Start createPublication ", language, filename);
        try {
            db.startUpdate();
            DbVersion dbver = getDbVersion();

            String next_name = getNextPublicationName(dbver);
            DbPublicationExport newpub = new DbPublicationExport();
            newpub.setExportName(next_name);
            String now_millis = Long.toString(System.currentTimeMillis());
            Map attribs = newpub.getAttributes();
            attribs.put(ATTRIBUTE_PUBLICATION_FILENAME, filename);
            attribs.put(ATTRIBUTE_PUBLICATION_LANGUAGE, language);
            attribs.put(ATTRIBUTE_PUBLICATION_CREATION_TIME, now_millis);
            dbver.addPublicationExport(newpub);

            db.commit();
            debug("-- End createPublication");
            return next_name;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized String createPublication(String publicationId, String language, String filename)
    {
        debug("-- Start createPublication ", publicationId, language, filename);
        try {
            db.startUpdate();
            DbVersion dbver = getDbVersion();

            String real_name;
            if (publicationNameExists(dbver, publicationId)) {
                real_name = getNextPublicationName(dbver);
            } else {
                real_name = publicationId;
            }
            DbPublicationExport newpub = new DbPublicationExport();
            newpub.setExportName(real_name);
            String now_millis = Long.toString(System.currentTimeMillis());
            Map attribs = newpub.getAttributes();
            attribs.put(ATTRIBUTE_PUBLICATION_FILENAME, filename);
            attribs.put(ATTRIBUTE_PUBLICATION_LANGUAGE, language);
            attribs.put(ATTRIBUTE_PUBLICATION_CREATION_TIME, now_millis);
            dbver.addPublicationExport(newpub);

            db.commit();
            debug("-- End createPublication");
            return real_name;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized void deletePublication(String publicationId)
    {
        debug("-- Start deletePublication ", publicationId);
        try {
            db.startUpdate();
            DbVersion dbver = getDbVersion();
            dbver.removePublicationExport(publicationId);
            db.commit();
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
        }
        debug("-- End deletePublication");
    }

    public synchronized String[] listPublications()
    {
        debug("-- Start listPublications");
        try {
            db.startRead();
            DbVersion dbver = getDbVersion();
            Set pubs = dbver.allPublicationExports();
            List list = new ArrayList(pubs.size());
            Iterator it = pubs.iterator();
            while (it.hasNext()) {
                DbPublicationExport exp = (DbPublicationExport) it.next();
                list.add(exp.getExportName());
            }
            db.commit();
            debug("-- End listPublications");
            return (String[]) list.toArray(new String[list.size()]);
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized String getAttribute(String publicationId, String attName)
    {
        debug("-- Start getAttribute ", publicationId, attName);
        try {
            db.startRead();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            String val = null;
            if (exp != null) {
                val = (String) exp.getAttributes().get(attName);
            }
            db.commit();
            debug("-- End getAttribute");
            return val;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized String[] getAttributeNames(String publicationId)
    {
        debug("-- Start getAttributeNames ", publicationId);
        try {
            db.startRead();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            String[] res = null;
            if (exp != null) {
                res = new String[exp.getAttributes().size()];
                res = exp.getAttributes().keySet().toArray(res);
            }
            db.commit();
            debug("-- End getAttributeNames");
            return res;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public synchronized void setAttribute(String publicationId, String attName, String attValue)
    {
        debug("-- Start setAttribute ", publicationId, attName, attValue);
        try {
            db.startUpdate();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            exp.getAttributes().put(attName, attValue);
            db.commit();
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
        }
        debug("-- End setAttribute");
    }

    public synchronized void setAttributes(String publicationId, String[] attNames, String[] attValues)
    {
        debug("-- Start setAttributes ", publicationId, attNames);
        try {
            db.startUpdate();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            Map attribs = exp.getAttributes();
            for (int i=0; i < attNames.length; i++) {
                attribs.put(attNames[i], attValues[i]);
            }
            db.commit();
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
        }
        debug("-- End setAttributes");
    }

    public InputStream readPublicationStream(String publicationId)
    {
        debug("-- Start readPublicationStream ", publicationId);
        // Write blob to file.
        // This is necessary because the blob returned from hibernate is
        // not materialized, i.e. it can only be accessed as long as the
        // the database transaction is not closed.
        File tmpfile = null;
        // synchronized (this) {
        
        // Create temporary db session object to avoid need of synchronization.
        DbWork temp_work = new DbWork(dbFactory, 0);
        try {
            temp_work.startRead();
            DbVersion dbver = getDbVersion(temp_work);
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            if (exp != null) {
                Blob blob = exp.getExportFile();
                if (blob != null) {
                    InputStream instream = blob.getBinaryStream();
                    if (instream != null) {
                        tmpfile = createTempFile("tmp");
                        try {
                            DocmaUtil.writeStreamToFile(instream, tmpfile);
                        } finally {
                            instream.close();
                        }
                    }
                }
            }
            temp_work.commit();
        } catch (Exception ex) {
            temp_work.rollbackRuntime(ex);   // rollback and throw runtime exception
        } finally {
            temp_work.close();
        }
        // }

        // Create Inputstream from file
        debug("-- End readPublicationStream ", tmpfile);
        if ((tmpfile != null) && tmpfile.exists()) {
            try {
                return new FileInputStream(tmpfile);
            } catch (Exception ex) {
                throw new DocRuntimeException(ex);
            }
        } else {
            return null;
        }
    }

    public OutputStream openPublicationOutputStream(String publicationId)
    {
        debug("-- Start openPublicationOutputStream ", publicationId);
        DbWork temp_work = new DbWork(dbFactory, 0);
        try {
            Session dbSess = temp_work.startUpdate();
            DbVersion dbver = getDbVersion(temp_work);
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);

            Blob blob; // = exp.getExportFile();
            // if (blob == null) {
                final byte[] dummy_arr = {};
                blob = dbSess.getLobHelper().createBlob(dummy_arr);
                exp.setExportFile(blob);
                // Following is a workaround, because the Blob created by 
                // Hibernate LobHelper does not support all stream operations 
                // (i.e. otherwise following exception is thrown:
                // java.lang.UnsupportedOperationException: Blob may not be manipulated from creating session) 
                // if (blob.getClass().getName().contains("$Proxy")) {  // workaround required for PostgreSQL
                //     dbSess.flush();       // make blob persistent
                //     exp.refreshExportFileBlob(dbSess);  // read persisted blob from DB
                //     blob = exp.getExportFile();  // get persisted blob 
                // }
            // }
            // Clear Blob content 
            // if (blob.length() > 0) blob.truncate(0);

            OpenedStreamEntry entry = new OpenedStreamEntry();
            entry.dbWork = temp_work;
            entry.pubExport = exp;
            try {
                entry.outStream = blob.setBinaryStream(1);  // according to api documentation,
                                                            // first byte is at position 1
            } catch (Exception ex) {  // UnsupportedOperationException
                if (DocConstants.DEBUG) {
                    Log.info(ex.getMessage());
                }
                // Stream operation not supported. Workaround: read stream into memory.
                entry.outStream = new InMemoryOutputStream(entry);
            }
            outStreamMap.put(publicationId, entry);
            debug("-- End openPublicationOutputStream ", publicationId);
            return entry.outStream;
            // Note: There is no temp_work.commit() here. The commit is executed
            // in the method closePublicationOutputStream().
            // Therefore it is important that every(!) call to this method
            // is followed by a call to closePublicationOutputStream(),
            // even if there is an error inbetween (i.e. put the
            // closePublicationOutputStream() call in a finally block).
        } catch (Exception ex) {
            outStreamMap.remove(publicationId);
            temp_work.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        }
    }

    public void closePublicationOutputStream(String publicationId)
    {
        debug("-- Start closePublicationOutputStream ", publicationId);
        OpenedStreamEntry entry = (OpenedStreamEntry) outStreamMap.get(publicationId);
        if (entry != null) {
            try {
                entry.outStream.close();
                Session dbSess = entry.dbWork.getSession();
                dbSess.flush();  // make blob persistent
                long len = entry.pubExport.getExportFile().length();
                entry.pubExport.setExportSize(len);
                String now_millis = Long.toString(System.currentTimeMillis());
                entry.pubExport.getAttributes().put(ATTRIBUTE_PUBLICATION_CLOSING_TIME, now_millis);
                entry.dbWork.commit();
            } catch (Exception ex) {
                entry.dbWork.rollbackRuntime(ex);   // rollback and throw runtime exception
            } finally {
                entry.dbWork.close();
            }
        }
        refresh(publicationId);  // refresh cache because update was done in a temporary session
        debug("-- End closePublicationOutputStream ", publicationId);
    }

    public boolean hasPublicationStream(String publicationId)
    {
        debug("-- Start hasPublicationStream ", publicationId);
        String closing_time = getAttribute(publicationId, ATTRIBUTE_PUBLICATION_CLOSING_TIME);
        debug("-- End hasPublicationStream ", publicationId, closing_time);
        return (closing_time != null) && !closing_time.equals("");
    }

    public synchronized long getPublicationSize(String publicationId)
    {
        debug("-- Start getPublicationSize ", publicationId);
        try {
            db.startRead();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            long sz = exp.getExportSize();
            db.commit();
            debug("-- End getPublicationSize ", publicationId, sz);
            return sz;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return 0;
        }
    }

    public DocmaExportLog readExportLog(String publicationId)
    {
        debug("-- Start readExportLog ", publicationId);
        // Create temporary db session object to avoid need of synchronization.
        DbWork temp_work = new DbWork(dbFactory, 0);
        try {
            temp_work.startRead();
            DbVersion dbver = getDbVersion(temp_work);
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            DocmaExportLog explog = null;
            Blob blob = exp.getExportLog();
            if (blob != null) {
                InputStream instream = blob.getBinaryStream();
                if (instream != null) {
                    try {
                        explog = DocmaExportLog.loadFromXML(instream);
                    } finally {
                        instream.close();
                    }
                }
            }
            temp_work.commit();
            debug("-- End readExportLog ", publicationId);
            return explog;
        } catch (Exception ex) {
            temp_work.rollbackRuntime(ex);   // rollback and throw runtime exception
            return null;
        } finally {
            temp_work.close();
        }
    }

    public void writeExportLog(String publicationId, DocmaExportLog log)
    {
        debug("-- Start writeExportLog ", publicationId);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(64*1024);
        try {
            log.storeToXML(bout);
        } catch (IOException ex) {
            throw new DocRuntimeException(ex);
        }
        synchronized (this) {
            try {
                Session dbSess = db.startUpdate();
                DbVersion dbver = getDbVersion();
                DbPublicationExport exp = dbver.getPublicationExport(publicationId);
                // Blob blob = exp.getExportLog();
                // if (blob == null) {
                Blob blob = dbSess.getLobHelper().createBlob(bout.toByteArray());
                exp.setExportLog(blob);
                // } else {
                //     blob.setBytes(1, bout.toByteArray());
                // }
                db.commit();
            } catch (Exception ex) {
                db.rollbackRuntime(ex);   // rollback and throw runtime exception
            }
        }
        debug("-- End writeExportLog ", publicationId);
    }

    public synchronized boolean hasExportLog(String publicationId)
    {
        debug("-- Start hasExportLog ", publicationId);
        try {
            db.startRead();
            DbVersion dbver = getDbVersion();
            DbPublicationExport exp = dbver.getPublicationExport(publicationId);
            if (exp.getPubExportDbId() > 0) {
                db.getSession().refresh(exp);   // required for PostgreSQL 
            }
            boolean haslog = exp.hasExportLog();
            db.commit();
            debug("-- End hasExportLog ", publicationId, haslog);
            return haslog;
        } catch (Exception ex) {
            db.rollbackRuntime(ex);   // rollback and throw runtime exception
            return false;
        }
    }

    public synchronized void refresh(String publicationId)
    {
        debug("-- Refresh ", publicationId);
        refreshFlag = true;
    }
    
    public synchronized void invalidateCache()
    {
        debug("-- invalidateCache");
        refreshFlag = true;
        versionDbId = null;   // reload cached database id
    }
    
    public synchronized void close()
    {
        debug("-- close");
        if (db != null) {
            db.close();
        }
    }

    /* --------------  Private methods  --------------- */

    private void debug(Object... args)
    {
        if (DocConstants.DEBUG) {
            StringBuilder msg; 
            msg = new StringBuilder(this.getClass().getName()); 
            msg.append(", Store: ").append(docStoreId); 
            msg.append(", Version: ").append(this.versionId.toString());
            for (Object obj : args) {
                msg.append(" ").append((obj == null) ? "null" : obj.toString());
            }
            System.out.println(msg.toString());
        }
    }
    
    private String getNextPublicationName(DbVersion dbver)
    {
        int max = -1;
        Iterator it = dbver.allPublicationExports().iterator();
        while (it.hasNext()) {
            DbPublicationExport dbpub = (DbPublicationExport) it.next();
            String pubname = dbpub.getExportName();
            if ((pubname != null) && pubname.startsWith(PUB_PREFIX)) {
                String seq_str = pubname.substring(PUB_PREFIX.length());
                try {
                    int num = Integer.parseInt(seq_str);
                    if (num > max) max = num;
                } catch (Exception ex) {}
            }
        }
        int next_num = (max == -1) ? 1 : (max + 1);
        return PUB_PREFIX + next_num;
    }
    
    private boolean publicationNameExists(DbVersion dbver, String checkName)
    {
        Iterator it = dbver.allPublicationExports().iterator();
        while (it.hasNext()) {
            DbPublicationExport dbpub = (DbPublicationExport) it.next();
            String pubname = dbpub.getExportName();
            if ((pubname != null) && pubname.equalsIgnoreCase(checkName)) {
                return true;
            }
        }
        return false;
    }

    private DbVersion getDbVersion()
    {
        return getDbVersion(this.db);
    }
    
    private DbVersion getDbVersion(DbWork db_work)
    {
        Session dbSess = db_work.getSession();
        if (refreshFlag) {
            dbSess.clear();   // remove all loaded objects from cache
            refreshFlag = false;
        }
        DbVersion dbver = null;
        if (versionDbId != null) {
            if (DocConstants.DEBUG) {
                Log.info("PublicationArchive: Getting version object for database ID '" + versionDbId + "'.");
            }
            try {
                dbver = (DbVersion) dbSess.load(DbVersion.class, versionDbId);
                DocVersionId loadedVerId = versionIdFactory.createVersionId(dbver.getVersionName());
                String loadedStoreId = dbver.getStore().getStoreDisplayId();
                if ((! versionId.equals(loadedVerId)) || ! docStoreId.equals(loadedStoreId)) {
                    // Obviously the version id or store id for the cached 
                    // database id has changed -> database id is no longer valid.
                    // Note that the database id for a given version id could change,
                    // because changing the version id of a version is allowed
                    // as long as the version is in draft state.
                    versionDbId = null;  // reload
                    dbver = null;
                    if (DocConstants.DEBUG) {
                        Log.warning("PublicationArchive ID mismatch: " + loadedStoreId + ", " + loadedVerId);
                    }
                }
            } catch (Exception ex) {
                // No object found for cached database id (or some other problem)
                versionDbId = null;
                dbver = null;
            }
        }

        if (dbver == null) {  // Cached database id of version was no longer valid
            if (DocConstants.DEBUG) {
                Log.info("PublicationArchive: Getting version object for store '" + docStoreId + 
                         "' version '" + versionId + "'.");
            }
            // Reload version based on storeId, versionId
            DbStore dbstore = DbUtil.getDbStore(dbSess, docStoreId);
            dbver = DbUtil.getDbVersion(dbstore, versionId, versionIdFactory);
            versionDbId = new Integer(dbver.getVersionDbId());
        }
        return dbver;
    }

    private File createTempFile(String file_extension)
    {
        deleteOldTempFiles();
        File f;
        long stamp = System.currentTimeMillis();
        do {
            f = new File(tempDir, TEMPFILE_PREFIX + stamp + "." + file_extension);
            stamp++;
        } while (f.exists());
        return f;
    }


    private void deleteOldTempFiles()
    {
        if (tempDeleteCounter == 0) {  // delete old temp files after every 10th call
            if (++tempDeleteCounter >= 10) tempDeleteCounter = 0;

            long currentTime = System.currentTimeMillis();
            String[] filenames = tempDir.list();
            int pref_len = TEMPFILE_PREFIX.length();
            for (int i=0; i < filenames.length; i++) {
                String fn = filenames[i];
                if (fn.startsWith(TEMPFILE_PREFIX)) {
                    int p = fn.indexOf('.');
                    String num = (p < 0) ? fn.substring(pref_len) : fn.substring(pref_len, p);
                    try {
                        long stamp = Long.parseLong(num);
                        if ((currentTime - stamp) > (2 * 24 * 60 * 60 * 1000)) { // delete after 2 days
                            File del_file = new File(tempDir, fn);
                            // if (del_file.isDirectory()) {
                            //     DocmaUtil.recursiveFileDelete(del_file);
                            // } else {
                            del_file.delete();
                            // }
                        }
                    } catch (Exception ex) {}
                }
            }
        }
    }

}
