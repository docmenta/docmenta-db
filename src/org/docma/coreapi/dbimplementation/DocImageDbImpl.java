/*
 * DocImageDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import java.io.*;
import java.sql.Blob;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;


/**
 *
 * @author MP
 */
public class DocImageDbImpl extends DocContentDbImpl implements DocImage
{

    DocImageDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
    }

    private DbImageRendition getDbRendition(DocImageRendition rendition) throws Exception
    {
        DocNodeContext ctx = getDocContext();
        String lang_mode = ctx.getTranslationMode();
        String lang_key = (lang_mode == null) ? LANG_ORIG : lang_mode;
        DbNode nd = getDbNodeReal();
        DbBinaryContent cont = nd.getBinaryContent(lang_key);
        if ((lang_mode != null) && (cont == null)) {
            // if in translation mode, but no translated image exists, then read original image
            lang_key = LANG_ORIG;
            cont = nd.getBinaryContent(lang_key);
        }
        DbImageRendition rend = nd.getImageRendition(lang_key, rendition.getName());
        if ((rend == null) && (cont != null)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(24*1024);
            Blob image_blob = cont.getNodeContent(ctx);
            if (image_blob != null) {
                InputStream image_in = image_blob.getBinaryStream();
                ImageHelper.createRendition(out, image_in, rendition);
                try { image_in.close(); } catch (Exception ex) {}
                out.close();

                prepareUpdate(nd);  // allow update
                rend = nd.createImageRendition(lang_key, rendition.getName());
                rend.setContent(out.toByteArray());
                String mimetype = DocImageRendition.getMIMETypeFromFormat(rendition.getFormat());
                rend.setContentType(mimetype);
                rend.setMaxHeight(rendition.getMaxHeight());
                rend.setMaxWidth(rendition.getMaxWidth());
                // dbSess.save(rend);   // gives exception if nd is transient
            }
        }
        return rend;
    }


    private void deleteRenditions(String lang_code)
    {
        DbNode nd = getDbNode();
        prepareUpdate(nd);  // allow update
        String lang_key = (lang_code == null) ? LANG_ORIG : lang_code;
        nd.removeImageRenditions(lang_key, null);
    }

    private void deleteAllRenditions()
    {
        DbNode nd = getDbNode();
        prepareUpdate(nd);  // allow update
        nd.removeImageRenditions(null, null);
    }

    /* --------------  Interface DocImage ---------------------- */


    public void setContentFile(File img, String mimeType) throws DocException
    {
        throw new DocException("Operation setContentFile not supported yet.");
    }

    public byte[] getRendition(DocImageRendition rendition) throws DocException
    {
        byte[] res = null;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                DbImageRendition db_rend = getDbRendition(rendition);
                if (db_rend != null) {
                    res = db_rend.getContent();
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRethrow(started, ex);
            }
        }
        return res;
    }

    public InputStream getRenditionStream(DocImageRendition rendition) throws DocException
    {
        byte[] buf = getRendition(rendition);
        return new ByteArrayInputStream(buf);
    }

    public void setContentType(String mime_type)
    {
        if (! mime_type.startsWith("image")) {
            throw new DocRuntimeException("MIME-Type of an image must be an image type.");
        }
        super.setContentType(mime_type);
    }

    public void setContentStream(InputStream dataStream)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                super.setContentStream(dataStream);
                deleteRenditions(ctx.getTranslationMode());
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
    }

    public void deleteContent(String lang_code)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                super.deleteContent(lang_code);
                if (lang_code == null) {
                    deleteAllRenditions();
                } else {
                    deleteRenditions(lang_code);
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
    }

//    public void deleteTranslation(String lang_code)
//    {
//        DocNodeContext ctx = getDocContext();
//        synchronized (ctx) {
//            boolean started = startLocalTransaction();
//            try {
//                super.deleteTranslation(lang_code);
//                deleteRenditions(ctx.getTranslationMode());
//                commitLocalTransaction(started);
//            } catch (Exception ex) {
//                rollbackLocalTransactionRuntime(started, ex);
//            }
//        }
//    }

}
