/*
 * OpenedStreamEntry.java
 */

package org.docma.app.dbimplementation;

import java.io.*;
import org.docma.hibernate.DbWork;
import org.docma.coreapi.dbimplementation.dblayer.DbPublicationExport;

/**
 *
 * @author MP
 */
class OpenedStreamEntry
{
    DbWork dbWork = null;
    DbPublicationExport pubExport = null;
    OutputStream outStream = null;
}
