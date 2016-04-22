/*
 * DocContentDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import java.io.*;
import java.util.*;
import java.sql.Blob;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.lockapi.*;
import org.docma.util.Log;
import org.docma.util.DocmaUtil;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;

import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DocContentDbImpl extends DocNodeDbImpl implements DocContent
{
    private static List<TmpFileEntry> tmpFiles = new ArrayList<TmpFileEntry>();

    DocContentDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
    }

    /* --------------  Interface DocContent ---------------------- */

    public String getContentType()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            if (DocConstants.DEBUG) { 
                Log.info("Local transaction for getContentType: " + started + ", " + ctx.getDbTransactionCounter());
            }
            try {
                String lang = ctx.getTranslationMode();
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                DbBinaryContent bin = nd.getBinaryContent(lang_key);
                String res = (bin == null) ? null : bin.getContentType();
                if ((lang != null) && ((res == null) || (res.length() == 0))) {  // if no content type in translation mode
                    bin = nd.getBinaryContent(LANG_ORIG);  // read original content 
                    res = (bin == null) ? null : bin.getContentType();
                }
                commitLocalTransaction(started);
                return (res == null) ? "" : res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }


    public String getContentType(String lang)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            if (DocConstants.DEBUG) { 
                Log.info("Local transaction for getContentType: " + started + ", " + ctx.getDbTransactionCounter());
            }
            try {
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                DbBinaryContent bin = nd.getBinaryContent(lang_key);
                String res = (bin == null) ? null : bin.getContentType();
                commitLocalTransaction(started);
                if (lang == null) {  // for original language...
                    return (res == null) ? "" : res;   // ...return empty string as default
                } else {   // for translation language...
                    // ...return null to indicate that no translated value exists 
                    return ((res == null) || (res.length() == 0)) ? null : res;
                }
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }


    public void setContentType(String mime_type)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                if (lang == null) lang = LANG_ORIG;

                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                DbBinaryContent cont = nd.getBinaryContent(lang);
                if (cont == null) {
                    cont = nd.createBinaryContent(lang);
                    // dbSess.save(cont);
                } else {
                    prepareUpdate(cont);  // allow update
                }
                cont.setContentType(mime_type);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
    }


    public String getFileExtension()
    {
        return getAttribute(DocAttributes.FILE_EXTENSION);
    }


    public String getFileExtension(String lang)
    {
        return getAttribute(DocAttributes.FILE_EXTENSION, lang);
    }


    public void setFileExtension(String file_extension)
    {
        setAttribute(DocAttributes.FILE_EXTENSION, file_extension);
    }


    public byte[] getContent()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                DbBinaryContent bin = nd.getBinaryContent(lang_key);
                if ((bin == null) && (lang != null)) {  // if no translated content in translation mode
                    bin = nd.getBinaryContent(LANG_ORIG);  // read original content
                }
                byte[] res = null;
                if (bin != null) {
                    Blob cont = bin.getNodeContent(ctx);
                    if (cont != null) {
                        long len = cont.length();
                        if (len > Integer.MAX_VALUE) {
                            throw new DocRuntimeException("Content is too large to be retrieved as an array. Use stream instead.");
                        }
                        res = cont.getBytes(1, (int) len);
                    }
                }
                commitLocalTransaction(started);
                return (res == null) ? new byte[0] : res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }


    public void setContent(byte[] content)
    {
        // Simple implementation:
        // setContentStream(new ByteArrayInputStream(content));

        // Complex implementation:
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                if (lang == null) lang = LANG_ORIG;

                Session dbSess = getDbSession();
                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                DbBinaryContent cont = nd.getBinaryContent(lang);
                if (cont == null) {
                    cont = nd.createBinaryContent(lang);
                    // dbSess.flush();
                    Blob newlob = dbSess.getLobHelper().createBlob(content);
                    cont.setNodeContent(newlob, ctx);
                    cont.setContentLength(content.length);
                    // dbSess.save(cont);
                } else {
                    prepareUpdate(cont);  // allow update
                    Blob newlob = dbSess.getLobHelper().createBlob(content);
                    cont.setNodeContent(newlob, ctx);
                    cont.setContentLength(content.length);
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        // fireChangedEvent();
    }


    public InputStream getContentStream() 
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean local_transaction = startLocalTransaction();
            if (DocConstants.DEBUG && local_transaction) {
                Log.info("DocContentDbImpl.getContentStream() is called outside of transaction context.");
            }
            try {
                String lang = ctx.getTranslationMode();
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                DbBinaryContent bin = nd.getBinaryContent(lang_key);
                if ((bin == null) && (lang != null)) {  // if no translated content in translation mode
                    bin = nd.getBinaryContent(LANG_ORIG);  // read original content
                }
                InputStream res = null;
                if (bin != null) {
                    Blob cont = bin.getNodeContent(ctx);
                    if (cont != null) {
                        // Note: If getContentStream() is called in a local transaction,
                        // then the blob stream returned from the database cannot be
                        // directly returned as a result, because reading the blob stream
                        // is only allowed as long as the database transaction/session is
                        // open. Therefore, in a local transaction the complete content is 
                        // transferred into the memory or into a file and the returned
                        // stream is reading from the memory/file.
                        if (local_transaction) {
                            long len = bin.getContentLength();
                            if (len < 16*1024*1024) {  
                                // if content length is less than 16MB, then create in-memory stream
                                len = cont.length();
                                res = new ByteArrayInputStream(cont.getBytes(1, (int) len));
                            } else {
                                // otherwise write blob to temporary file and create stream from file
                                if (DocConstants.DEBUG && local_transaction) {
                                    Log.info("Creating temporary file for DocContentDbImpl.getContentStream().");
                                }
                                InputStream blobstream = cont.getBinaryStream();
                                if (blobstream != null) {
                                    res = new FileInputStream(writeStreamToTempFile(blobstream));
                                }
                            }
                        } else {
                            res = cont.getBinaryStream();
                        }
                    }
                }
                commitLocalTransaction(local_transaction);
                if (res == null) {
                    res = new ByteArrayInputStream(new byte[0]);
                }
                return res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(local_transaction, ex);
                return null;  // is never reached
            }
        }
    }


    public void setContentStream(InputStream instream)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                if (lang == null) lang = LANG_ORIG;

                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                DbBinaryContent cont = nd.getBinaryContent(lang);
                if (cont == null) {
                    cont = nd.createBinaryContent(lang);
                    // getDbSession().flush(); 
                    // or: dbSess.save(cont);   // gives exception if nd is transient
                }
                prepareUpdate(cont);  // allow update
                setBlobFromStream(instream, cont, ctx);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        // fireChangedEvent();
    }
    
//    private void setBlobFromStream(InputStream instream, DbBinaryContent cont, DocNodeContext ctx)
//    throws Exception
//    {
//        final int DEFAULT_BUF_SIZE = 8192;  // 8 KB
//        final int MAX_BUF_SIZE = 2621440;   // 2.5 MB
//        
//        // Read available bytes into buffer startbuf
//        int avail = instream.available();
//        final int startlen = ((avail > MAX_BUF_SIZE) || (avail == 0)) ? DEFAULT_BUF_SIZE : avail;
//        byte[] startbuf = new byte[startlen];
//        int cnt;
//        do { 
//            cnt = instream.read(startbuf); 
//        } while (cnt == 0);
//        boolean end_reached = (cnt < 0);
//        long total_length = (cnt > 0) ? cnt : 0;
//        
//        Blob lob = cont.getNodeContent(ctx);  // this method always returns a persisted Blob!
//        byte[] buf = null;
//        int offset = 1; // 1st byte in Blob has position 1
//        if (lob == null) {   // New Blob object has to be created
//            if (cnt > 0) {
//                if (cnt < startbuf.length) {
//                    startbuf = Arrays.copyOf(startbuf, cnt);   // trim buffer
//                }
//                lob = getDbSession().getLobHelper().createBlob(startbuf);
//                cont.setNodeContent(lob, ctx);
//                // dbSess.flush();  // make blob persistent
//                int nextbyte = instream.read();
//                if (nextbyte < 0) {  // no more bytes
//                    cnt = -1;
//                    end_reached = true;
//                } else {   // further bytes exist that have to be appended
//                    buf = new byte[DEFAULT_BUF_SIZE];
//                    buf[0] = (byte) nextbyte;
//                    int c = instream.read(buf, 1, buf.length - 1);  // try to fill buffer
//                    if (c < 0) {
//                        cnt = 1;      // amount of bytes in buf
//                        end_reached = true;
//                    } else {
//                        cnt = 1 + c;  // amount of bytes in buf
//                    }
//                    total_length += cnt;             // amount of bytes read from input stream
//                    offset = startbuf.length + 1;    // append position (1st byte in Blob has position 1)
//                    lob = cont.getNodeContent(ctx);  // retrieve persisted(!) blob because bytes have to be appended
//                }
//            }
//        } else {  // Existing Blob has to be overwritten
//            buf = startbuf;
//        }
//        
//        // Existing Blob has to be overwritten or bytes have to be appended to newly created Blob.
//        if (cnt > 0) {   // cnt bytes in buf have to be written
//            OutputStream out = lob.setBinaryStream(offset);
//            try {
//                out.write(buf, 0, cnt);
//                if (! end_reached) {
//                    if (buf.length < 4096) buf = new byte[DEFAULT_BUF_SIZE];
//                    while ((cnt = instream.read(buf)) >= 0) {
//                        total_length += cnt;
//                        out.write(buf, 0, cnt);
//                    }
//                }
//            } finally {
//                out.close();
//            }
//        }
//        if ((lob != null) && (total_length < lob.length())) {
//            lob.truncate(total_length);
//        }
//        cont.setContentLength(total_length);
//    }


    private void setBlobFromStream(InputStream instream, DbBinaryContent cont, DocNodeContext ctx)
    throws Exception
    {
        Blob lob; // = cont.getNodeContent(ctx);  // always returns a persisted Blob!
        // if (lob == null) {
            byte[] dummy_arr = {};
            Session dbSess = getDbSession();
            lob = dbSess.getLobHelper().createBlob(dummy_arr);
            cont.setNodeContent(lob, ctx);
            // Following is a workaround, because the Blob proxy created by 
            // Hibernate LobHelper does not support all stream operations 
            // (i.e. otherwise following exception is thrown:
            // java.lang.UnsupportedOperationException: Blob may not be manipulated from creating session) 
            // Log.info("LOB class before: " + lob.getClass().getName());
            // if (lob.getClass().getName().contains("$Proxy")) {  // workaround required for PostgreSQL
            //     dbSess.flush();  // make blob persistent
            //     cont.refreshContent(dbSess);
            //     lob = cont.getContent();  // cont.getNodeContent(ctx);  // get persisted blob
            // }
        // }
        if (DocConstants.DEBUG) {
            Log.info("LOB class in setBlobFromStream: " + lob.getClass().getName());
        }
        OutputStream out;
        long total_length = 0;
        try {
            out = lob.setBinaryStream(1);
            byte[] buf = new byte[8*1024];
            int cnt;
            while ((cnt = instream.read(buf)) >= 0) {
                out.write(buf, 0, cnt);
                total_length += cnt;
            }
            out.close();
            if (total_length < lob.length()) {
                lob.truncate(total_length);
            }
        } catch (Exception ex) {  // UnsupportedOperationException
            if (DocConstants.DEBUG) {
                Log.info(ex.getMessage());
            }
            // Stream operation not supported. Workaround: read stream into memory.
            byte[] arr = DocmaUtil.readStreamToByteArray(instream);
            total_length = arr.length;
            lob = dbSess.getLobHelper().createBlob(arr); // new javax.sql.rowset.serial.SerialBlob(arr);
            cont.setNodeContent(lob, ctx);
        }
        cont.setContentLength(total_length);
    }


    public String getContentString()
    {
        return getContentString("UTF-8");
    }


    public String getContentString(String charsetName)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                int sz = (int) getContentLength();
                InputStream in = getContentStream();
                Reader char_in = new InputStreamReader(in, charsetName);
                StringWriter out = new StringWriter(sz + 1024);
                int cnt;
                char[] buf = new char[1024];
                while ((cnt = char_in.read(buf)) >= 0) out.write(buf, 0, cnt);
                String res = out.toString();
                in.close();
                commitLocalTransaction(started);
                return res;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }


    public void setContentString(String str)
    {
        setContentString(str, "UTF-8");
    }


    public void setContentString(String str, String charsetName)
    {
        try {
            setContent(str.getBytes(charsetName));
        } catch (UnsupportedEncodingException e) { throw new DocRuntimeException(e); }
    }


    public long getContentLength()
    {
        long len = 0;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                DbBinaryContent bin = nd.getBinaryContent(lang_key);
                if ((bin == null) && (lang != null)) {  // if no translated content in translation mode
                    bin = nd.getBinaryContent(LANG_ORIG);  // read original content
                }
                if (bin != null) {
                    len = bin.getContentLength();
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return len;
    }

    public void deleteContent()
    {
        deleteContent(getDocContext().getTranslationMode());
    }

    public void deleteContent(String lang_code)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang_key = (lang_code == null) ? LANG_ORIG : lang_code;

                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                Session dbSess = getDbSession();
                DbBinaryContent cont = nd.removeBinaryContent(lang_key);
                if ((cont != null) && cont.persisted()) {
                    dbSess.delete(cont);
                }

                if (lang_code == null) {  // if original content is deleted, also delete all translations
                    Iterator it = getBinLanguageCodes(null).iterator();
                    while (it.hasNext()) {
                        cont = nd.removeBinaryContent((String) it.next());
                        if ((cont != null) && cont.persisted()) { 
                            dbSess.delete(cont);
                        }
                    }
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
    }

    public boolean hasContent(String lang_code)
    {
        boolean res = false;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                if (lang_code == null) lang_code = LANG_ORIG;
                if (getDbNodeReal().getBinaryContent(lang_code) != null) res = true;
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return res;
    }

    protected Set getBinLanguageCodes(Set aset)
    {
        if (aset == null) aset = new HashSet();
        DbNode nd = getDbNodeReal();
        Iterator it = nd.allBinaryContent().keySet().iterator();
        while (it.hasNext()) {
            String lang_code = (String) it.next();
            if (! lang_code.equals(LANG_ORIG)) aset.add(lang_code);
        }
        return aset;
    }

    public String[] getTranslations()
    {
        Set lang_codes = null;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                lang_codes = getAttLanguageCodes();
                getBinLanguageCodes(lang_codes);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return (String[]) lang_codes.toArray(new String[lang_codes.size()]);
    }

    public boolean hasTranslation(String lang_code)
    {
        boolean res = false;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                res = hasContent(lang_code) || super.hasTranslation(lang_code);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return res;
    }

    public void deleteTranslation(String lang_code)
    {
        if ((lang_code == null) || lang_code.equals(LANG_ORIG)) {
            throw new DocRuntimeException("Null or original language code in method deleteTranslation().");
        }
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                deleteContent(lang_code);            // delete translated content
                super.deleteTranslation(lang_code);  // delete translated attribute values
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
    }

    public Lock getLock(String lockname)
    {
        return getDocStore().getLockManager().getLock(getId(), lockname);
    }

    public boolean setLock(String lockname, long timeout)
    {
        return getDocStore().getLockManager().setLock(getId(), lockname, getDocContext().getUserId(), timeout);
    }

    public boolean refreshLock(String lockname, long timeout)
    {
        return getDocStore().getLockManager().refreshLock(getId(), lockname, timeout);
    }

    public Lock removeLock(String lockname)
    {
        return getDocStore().getLockManager().removeLock(getId(), lockname);
    }


    public HistoryEntry[] getHistory()
    {
        return null; // not implemented yet
    }

    // ----------- Private methods -------------
    
    
    private File writeStreamToTempFile(InputStream instream) throws IOException
    {
        File tmpfile = createTempFile();
        try {
            DocmaUtil.writeStreamToFile(instream, tmpfile);
        } finally {
            instream.close();
        }
        return tmpfile;
    }
    
    private static synchronized File createTempFile() throws IOException
    {
        long now = System.currentTimeMillis();
        deleteOldTempFiles(now);
        TmpFileEntry entry = new TmpFileEntry();
        entry.creationTime = now;
        entry.tmpFile = File.createTempFile("cont", null);
        entry.tmpFile.deleteOnExit();
        tmpFiles.add(entry);
        return entry.tmpFile;
    }
    
    private static synchronized void deleteOldTempFiles(long now)
    {
        for (int i = tmpFiles.size() - 1; i >= 0; i--) {
            TmpFileEntry entry = tmpFiles.get(i);
            if (entry.creationTime < now - (12*60*60*1000)) {  // delete files after 12 hours
                if (entry.tmpFile.exists()) entry.tmpFile.delete();
                tmpFiles.remove(i);
            }
        }
    }


    private static class TmpFileEntry
    {
        long creationTime;
        File tmpFile;
    }
}
