/*
 * DocFileDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.*;

/**
 *
 * @author MP
 */
public class DocFileDbImpl extends DocContentDbImpl implements DocFile
{
    DocFileDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
    }

    /* --------------  Interface DocFile ---------------------- */

    public String getFileName()
    {
        String fn = null;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                String ext = getFileExtension();
                if ((ext == null) || (ext.length() == 0)) {
                    fn = getTitle();
                } else {
                    fn = getTitle() + "." + ext;
                }
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
        return fn;
    }

    public void setFileName(String filename)
    {
        String ext_new = "";
        String name_new = filename;
        int p = filename.lastIndexOf('.');
        if (p >= 0) {
            ext_new = filename.substring(p + 1);
            name_new = filename.substring(0, p);
        }

        // Set new metadata
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                setTitle(name_new);
                setFileExtension(ext_new);
                commitLocalTransaction(started);
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);
            }
        }
    }

}
