/*
 * DocTextDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import java.io.*;
import java.util.*;

import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.util.Log;
import org.docma.util.DocmaUtil;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;

import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DocTextDbImpl extends DocContentDbImpl
{

    DocTextDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
    }


    Reader getCharacterStream()
    {
        if (DocConstants.DEBUG && !getDocContext().runningTransaction()) {
            Log.warning("DocTextDbImpl.getCharacterStream() is called outside of transaction context.");
        }
        String str = getContentString();
        return new StringReader((str == null) ? "" : str);
    }


    /* --------------  Overwrite DocContent methods  ---------------------- */

    public String getContentType()
    {
        // return getAttribute(DocAttributes.CONTENT_TYPE);

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
                DbTextContent txt = nd.getTextContent(lang_key);
                String res = (txt == null) ? null : txt.getContentType();
                if ((lang != null) && ((res == null) || (res.length() == 0))) {  // if no content type in translation mode
                    txt = nd.getTextContent(LANG_ORIG);  // read original content
                    res = (txt == null) ? null : txt.getContentType();
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
                DbTextContent txt = nd.getTextContent(lang_key);
                String res = (txt == null) ? null : txt.getContentType();
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
        // setAttribute(DocAttributes.CONTENT_TYPE, mime_type);

        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                if (lang == null) lang = LANG_ORIG;

                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                DbTextContent cont = nd.getTextContent(lang);
                if (cont == null) {
                    cont = nd.createTextContent(lang);
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


    public byte[] getContent()
    {
        String str = getContentString();
        if (str == null) {
            return new byte[0];
        }
        try {
            return str.getBytes("UTF-8");
        } catch (Exception ex) {
            throw new DocRuntimeException (ex);
        }
    }


    public void setContent(byte[] content)
    {
        try {
            setContentString(new String(content, "UTF-8"));
        } catch (Exception ex) {
            throw new DocRuntimeException (ex);
        }
    }


    /**
     *
     * @return
     */
    public InputStream getContentStream()
    {
        byte[] cont = getContent();
        if (cont == null) {
            cont = new byte[0];
        }
        return new ByteArrayInputStream(cont);
    }


    public void setContentStream(InputStream instream)
    {
        try {
            ByteArrayOutputStream outstream = new ByteArrayOutputStream();
            DocmaUtil.copyStream(instream, outstream);
            outstream.close();
            setContentString(outstream.toString("UTF-8"));
        } catch (Exception ex) {
            throw new DocRuntimeException(ex);
        }
    }


    public String getContentString()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                String lang_key = (lang == null) ? LANG_ORIG : lang;
                DbNode nd = getDbNodeReal();
                if (DocConstants.DEBUG) {
                    Log.info("Getting text content of node " + nd.getNodeDbId() + ". Local transaction started: " + started);
                }
                DbTextContent txt = nd.getTextContent(lang_key);
                if ((txt == null) && (lang != null)) {  // if no translated content in translation mode
                    txt = nd.getTextContent(LANG_ORIG);  // read original content
                }
                String res = (txt == null) ? null : txt.getContent();
                commitLocalTransaction(started);
                return (res == null) ? "" : res;
            } catch (Exception ex) {
                if (DocConstants.DEBUG) {
                    Log.error("Exception in DocTextDbImpl.getContentString: " + ex.getMessage());
                    ex.printStackTrace();
                }
                rollbackLocalTransactionRuntime(started, ex);
                return null;  // is never reached
            }
        }
    }


    public void setContentString(String str)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String lang = ctx.getTranslationMode();
                if (lang == null) lang = LANG_ORIG;

                DbNode nd = activateDbNode();
                prepareUpdate(nd);  // allow update
                DbTextContent txt = nd.getTextContent(lang);
                if (txt == null) {
                    txt = nd.createTextContent(lang);
                    // dbSess.save(txt);  // gives exception if nd is transient
                }
                prepareUpdate(txt);  // allow update
                txt.setContent(str);
                // long str_len = (str == null) ? 0 : str.length();
                // txt.setContentLength(str_len);

                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
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
                DbTextContent txt = nd.getTextContent(lang_key);
                if ((txt == null) && (lang != null)) {  // if no translated content in translation mode
                    txt = nd.getTextContent(LANG_ORIG);  // read original content
                }
                if (txt != null) {
                    len = txt.getContentLength();
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return len;
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
                DbTextContent cont = nd.removeTextContent(lang_key);
                if ((cont != null) && cont.persisted()) { 
                    dbSess.delete(cont); 
                }

                if (lang_code == null) {  // if original content is deleted, also delete all translations
                    Iterator it = getTxtLanguageCodes(null).iterator();
                    while (it.hasNext()) {
                        cont = nd.removeTextContent((String) it.next());
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
                if (getDbNodeReal().getTextContent(lang_code) != null) res = true;
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return res;
    }

    protected Set getTxtLanguageCodes(Set aset)
    {
        if (aset == null) aset = new HashSet();
        DbNode nd = getDbNodeReal();
        Iterator it = nd.allTextContent().keySet().iterator();
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
                getTxtLanguageCodes(lang_codes);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        return (String[]) lang_codes.toArray(new String[lang_codes.size()]);
    }

}
