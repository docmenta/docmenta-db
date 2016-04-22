/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.docma.coreapi.dbimplementation;

import java.util.*;
import java.io.*;
import org.docma.app.dbimplementation.*;
import org.docma.coreapi.*;
import org.docma.coreapi.implementation.*;
import org.docma.coreapi.dbimplementation.dblayer.DbNode;
import org.docma.coreapi.dbimplementation.dblayer.DbStore;
import org.docma.coreapi.dbimplementation.dblayer.DbVersion;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;
import org.docma.hibernate.HibernateUtil;
import org.docma.util.DocmaUtil;
import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DbPubArchiveTest 
{
    private static SessionFactory fact;

    static final String[] STORE_IDS = { "myStoreName1" };
    static final String STORE_PROP_NAME1 = "store.prop.1";
    static final String STORE_PROP_NAME2 = "store.prop.2";
    static final String STORE_PROP_NAME3 = "store.prop.3";
    static final String STORE_PROP_NAME4 = "store.prop.4";
    static final String PROP_VAL1 = "Value 1 ";
    static final String PROP_VAL2 = "Value 2 ";
    static final String PROP_VAL3 = " Value 3 ";
    static final String PROP_VAL4 = "  Value 4  ";
    
    static final String[] VER_NAMES = { "1.0.0", "Draft" };
        
    static final String VER_PROP_NAME1 = "ver.prop.1";
    static final String VER_PROP_NAME2 = "ver.prop.2";
    static final String VER_PROP_NAME3 = "ver.prop.3";
    static final String VER_PROP_NAME4 = "ver.prop.4";
    static final String VER_PROP_VAL1 = "Value 1 ";
    static final String VER_PROP_VAL2 = "Value 2 ";
    static final String VER_PROP_VAL3 = " Value 3 ";
    static final String VER_PROP_VAL4 = "  Value 4  ";

    static final String CHILD1_ATT_A_NAME = "child1_att_a";
    static final String CHILD1_ATT_A_VALUE = "1. attribute of child1";
    static final String CHILD1_ATT_A_VALUE_TRANSLATED = "1. translated attribute of child1";
    static final String CHILD1_ATT_B_NAME = "child1_att_b";
    static final String CHILD1_ATT_B_VALUE = "2. attribute of child1";
    static final String CHILD1_ATT_B_VALUE_TRANSLATED = "2. translated attribute of child1";
    static final String CHILD1_ATT_C_NAME = "child1_att_c";
    static final String CHILD1_ATT_C_VALUE = "3. attribute of child1";
    static final String CHILD1_ATT_C_VALUE_TRANSLATED = "3. translated attribute of child1";


    public static void main(String[] args) throws Exception
    {
        // HibernateUtil.setExternalConnectionProperties(new File("C:\\Arbeit\\netbeans_projekte\\DocmentaApp\\web\\WEB-INF\\db_config.properties"));
        
        fact = DbTestUtil.setUp_Derby("c:/work/derby_test_dbs/db_impl_test/dbstore");
        
        // fact = DbTestUtil.setUp_Oracle("jdbc:oracle:thin:@192.168.1.199:1521", "docmatest", "docmapw");
        
//        final String CONNECTION_URL = "jdbc:sqlserver://192.168.1.199:1433;databaseName=DOCMATEST;";
//        final String DB_USER = "docmauser";
//        final String DB_PASSWD = "docmapw";
//        fact = DbTestUtil.setUp_SQLServer(CONNECTION_URL, DB_USER, DB_PASSWD);
        
        createStore();
        testPubArchive();
    }
    
    private static void testPubArchive() throws Exception
    {
        VersionIdFactory verfact = new MyVersionIdFactory();
        File tempDir = new File("C:\\work\\docmenta_base\\temp");
        DocVersionId vid = verfact.createVersionId(VER_NAMES[0]);
        PublicationArchiveDbImpl arch = new PublicationArchiveDbImpl(STORE_IDS[0], vid, verfact, fact, tempDir);
        
        String targetPubId = arch.createPublication("publication1", "en", "reference_manual_1-6_pdf.pdf");
        
        File fpath = new File("C:\\TEMP\\reference_manual_1-6_pdf.pdf");
        FileInputStream in = new FileInputStream(fpath);
        OutputStream out = null;
        try {
            out = arch.openPublicationOutputStream(targetPubId);
            DocmaUtil.copyStream(in, out);
        } finally {
            if (out != null) {
                arch.closePublicationOutputStream(targetPubId); 
            }
            if (in != null) { 
                in.close();
            }
        }
        arch.close();
        
        PublicationArchiveDbImpl arch2 = new PublicationArchiveDbImpl(STORE_IDS[0], vid, verfact, fact, tempDir);
        InputStream in2 = arch2.readPublicationStream(targetPubId);
        byte[] buf = new byte[4096];
        int cnt;
        long total = 0;
        while ((cnt = in2.read(buf)) >= 0) {
            total += cnt;
        }
        in2.close();
        arch2.close();
        if (total != fpath.length()) {
            throw new Exception("Publication has invalid length: " + total);
        }
        System.out.println("Archived publication length: " + total);
    }
    
    private static void createStore()
    {
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            for (int i=0; i < STORE_IDS.length; i++) {
                String display_id = STORE_IDS[i];
                log("Creating store " + display_id);
                DbStore store = new DbStore();
                store.setStoreDisplayId(display_id);
                
                Map store_props = store.getProperties();
                store_props.put(STORE_PROP_NAME1, display_id + ":" + PROP_VAL1);
                store_props.put(STORE_PROP_NAME2, display_id + ":" + PROP_VAL2);
                store_props.put(STORE_PROP_NAME3, display_id + ":" + PROP_VAL3);
                store_props.put(STORE_PROP_NAME4, display_id + ":" + PROP_VAL4);
                                
                sess.save(store);    // make persistent (connect store with session)
                
                for (int v=0; v < VER_NAMES.length; v++) {
                    String vname = VER_NAMES[v];
                    log("Creating version " + vname);
                    DbVersion ver = new DbVersion();
                    ver.setVersionName(vname);
                    store.addVersion(ver);

                    Map ver_props = ver.getProperties();
                    ver_props.put(VER_PROP_NAME1, vname + ":" + VER_PROP_VAL1);
                    ver_props.put(VER_PROP_NAME2, vname + ":" + VER_PROP_VAL2);
                    ver_props.put(VER_PROP_NAME3, vname + ":" + VER_PROP_VAL3);
                    ver_props.put(VER_PROP_NAME4, vname + ":" + VER_PROP_VAL4);
                    
                    log("Creating root node");
                    DbNode root1 = ver.createNode();
                    ver.addRootNode(root1);

                    log("Creating level 1 nodes");
                    DbNode child1 = ver.createNode();
                    DbNode child2 = ver.createNode();
                    root1.addChildNode(child1);
                    root1.addChildNode(child2);

                    log("Creating level 2 nodes");
                    DbNode subchild1 = ver.createNode();
                    DbNode subchild2 = ver.createNode();
                    DbNode subchild3 = ver.createNode();
                    // DbNode subchild4 = ver.createNode();
                    child1.addChildNode(subchild1);
                    child1.addChildNode(subchild2);
                    child2.addChildNode(subchild3);
                    // child2.addChildNode(subchild4);

                    child1.setAttribute(LANG_ORIG,   CHILD1_ATT_A_NAME, CHILD1_ATT_A_VALUE);
                    child1.setAttribute("EN", CHILD1_ATT_A_NAME, CHILD1_ATT_A_VALUE_TRANSLATED);
                    child1.setAttribute(LANG_ORIG,   CHILD1_ATT_B_NAME, CHILD1_ATT_B_VALUE);
                    child1.setAttribute("EN", CHILD1_ATT_B_NAME, CHILD1_ATT_B_VALUE_TRANSLATED);
                    child1.setAttribute(LANG_ORIG,   CHILD1_ATT_C_NAME, CHILD1_ATT_C_VALUE);
                    child1.setAttribute("EN", CHILD1_ATT_C_NAME, CHILD1_ATT_C_VALUE_TRANSLATED);
                    
                    sess.save(ver);    // make persistent 
                }
            }
            tx.commit();
        } finally {
            sess.close();
        }
    }

    private static void log(String msg)
    {
        System.out.println("#### Test: " + msg);
    }

}
