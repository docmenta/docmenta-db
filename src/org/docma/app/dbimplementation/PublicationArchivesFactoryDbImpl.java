/*
 * PublicationArchivesFactoryDbImpl.java
 */

package org.docma.app.dbimplementation;

import java.io.*;
import org.hibernate.SessionFactory;

import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;


/**
 *
 * @author MP
 */
public class PublicationArchivesFactoryDbImpl implements PublicationArchivesFactory
{
    private SessionFactory factory;
    private File tempDir;


    public void PublicationArchivesFactoryDbImpl(SessionFactory factory, File tempDir)
    throws Exception
    {
        this.factory = factory;
        this.tempDir = tempDir;
    }

    public PublicationArchivesSession createSession(DocStoreSession sess)
    {
        VersionIdFactory verIdFact = ((AbstractDocStoreSession) sess).getVersionIdFactory();
        return new PublicationArchivesSessionDbImpl(factory, verIdFact, tempDir);
    }

}
