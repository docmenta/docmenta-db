/*
 * DocXMLDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.*;

import java.io.*;
import java.util.*;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author MP
 */
public class DocXMLDbImpl extends DocTextDbImpl implements DocXML
{
    private static DocumentBuilderFactory domFactory = null;
    private static TransformerFactory transformerFactory = null;

    private static Stack domBuilders = new Stack();
    private static Stack transformers = new Stack();

    DocXMLDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
        initOnce();
    }

    /* ----------------  Private methods  --------------------- */

    private static synchronized void initOnce()
    {
        if ((domFactory == null) || (transformerFactory == null)) {
            try {
                domFactory = DocumentBuilderFactory.newInstance();
                transformerFactory = TransformerFactory.newInstance();
            } catch (Exception ex) { 
                throw new DocRuntimeException(ex);
            }
        }
    }
    
    private static synchronized DocumentBuilder acquireDOMBuilder()
    {
        if (domBuilders.empty()) {
            try {
                return domFactory.newDocumentBuilder();
            } catch (Exception ex) { throw new DocRuntimeException(ex); }
        } else {
            return (DocumentBuilder) domBuilders.pop();
        }
    }
    
    private static void releaseDOMBuilder(DocumentBuilder dom_builder)
    {
        if (dom_builder != null) domBuilders.push(dom_builder);
    }

    private static synchronized Transformer acquireTransformer()
    {
        if (transformers.empty()) {
            try {
                return transformerFactory.newTransformer();
            } catch (Exception ex) { throw new DocRuntimeException(ex); }
        } else {
            return (Transformer) transformers.pop();
        }
    }

    private static void releaseTransformer(Transformer xml_transformer)
    {
        if (xml_transformer != null) transformers.push(xml_transformer);
    }


    /* --------------  Interface DocXML ---------------------- */

    public Document getContentDOM()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            DocumentBuilder domBuilder = acquireDOMBuilder();
            try {
                Reader cstream = getCharacterStream();
                InputSource is = new InputSource(cstream);
                Document doc = domBuilder.parse(is);
                try { cstream.close(); } catch (Exception ex) {}
                commitLocalTransaction(started);
                return doc;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
                return null;   // is never reached
            } finally {
                releaseDOMBuilder(domBuilder);
            }
        }
    }

    public void setContentDOM(Document xmldoc)
    {
        StringWriter buf = new StringWriter(32*1024);
        Transformer xmlTransformer = acquireTransformer();
        try {
            xmlTransformer.transform(new DOMSource(xmldoc), new StreamResult(buf));
        } catch (Exception ex) { 
            throw new DocRuntimeException(ex);
        } finally {
            releaseTransformer(xmlTransformer);
        }
        setContentString(buf.toString());
    }

}
