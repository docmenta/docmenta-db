/*
 * PublicationArchivesSessionDbImpl.java
 */

package org.docma.app.dbimplementation;

import java.io.*;
import java.util.*;
import org.hibernate.*;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.VersionIdFactory;

/**
 *
 * @author MP
 */
public class PublicationArchivesSessionDbImpl implements PublicationArchivesSession
{
    private Map<String, PublicationArchiveDbImpl> allArchives = new HashMap<String, PublicationArchiveDbImpl>();
    private VersionIdFactory versionIdFactory;
    private org.hibernate.SessionFactory dbFactory;
    private File tempDir;


    public PublicationArchivesSessionDbImpl(org.hibernate.SessionFactory dbFactory,
                                            VersionIdFactory verIdFactory,
                                            File tempDir)
    {
        this.dbFactory = dbFactory;
        this.versionIdFactory = verIdFactory;
        this.tempDir = tempDir;
    }

    public PublicationArchive getArchive(String storeId, DocVersionId verId)
    {
        String key = getGlobalArchiveKey(storeId, verId);
        PublicationArchiveDbImpl ar = allArchives.get(key);
        if (ar == null) {
            ar = new PublicationArchiveDbImpl(storeId, verId, versionIdFactory, dbFactory, tempDir);
            allArchives.put(key, ar);
        }
        return ar;
    }
    
    public void invalidateCache(String storeId) 
    {
        for (PublicationArchiveDbImpl ar : allArchives.values()) {
            if (storeId.equals(ar.getDocStoreId())) {
                ar.invalidateCache();
            }
        }
    }
    
    public void invalidateCache(String storeId, DocVersionId verId)
    {
        String key = getGlobalArchiveKey(storeId, verId);
        PublicationArchiveDbImpl ar = allArchives.get(key);
        if (ar != null) {
            ar.invalidateCache();
        }
    }


    private static String getGlobalArchiveKey(String storeId, DocVersionId verId)
    {
        return storeId + "#" + verId.toString();
    }
}
