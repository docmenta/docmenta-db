/*
 * InMemoryOutputStream.java
 */
package org.docma.app.dbimplementation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Blob;
import org.hibernate.Session;

/**
 *
 * @author MP
 */
public class InMemoryOutputStream extends ByteArrayOutputStream
{
    private OpenedStreamEntry openedEntry; 
    
    public InMemoryOutputStream(OpenedStreamEntry entry)
    {
        this.openedEntry = entry;
    }

    @Override
    public void close() throws IOException 
    {
        super.close(); 

        Session dbSess = openedEntry.dbWork.getSession();
        Blob lob = dbSess.getLobHelper().createBlob(this.toByteArray());
        openedEntry.pubExport.setExportFile(lob);
    }
    
    
}
