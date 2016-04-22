/*
 * DbImplTest.java
 */
package org.docma.coreapi.dbimplementation;

import java.util.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;
import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DbImplTest 
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
    
    public static void main(String[] args)
    {
        fact = DbTestUtil.setUp_Derby("c:/work/derby_test_dbs/db_impl_test/dbstore");
        createStore();
        readStore();
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
    
    private static void readStore()
    {
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            log("Read store: " + STORE_IDS[0]);
            tx = sess.beginTransaction();
            DbStore store = getDbStore(sess, STORE_IDS[0]);
            tx.commit();
            
            log("List all versions");
            tx.begin();
            String[] vnames = listVersionNames(store);
            tx.commit();
            
            String vname = VER_NAMES[0];
            log("Read version: " + vname);
            tx.begin();
            DbVersion ver = getDbVersion(store, vname);
            tx.commit();
            
            tx.begin();
            Map ver_props = ver.getProperties();
            log("Read version property: " + VER_PROP_NAME2);
            log("Value: " + ver_props.get(VER_PROP_NAME2));
            log("Read version property: " + VER_PROP_NAME3);
            log("Value: " + ver_props.get(VER_PROP_NAME3));
            log("Read version property: " + VER_PROP_NAME4);
            log("Value: " + ver_props.get(VER_PROP_NAME4));
            tx.commit();
            
            tx.begin();
            log("Get root node");
            DbNode root = (DbNode) ver.allRootNodes().get(0);
            log("Get child node");
            DbNode child1 = (DbNode) root.allChildNodes().get(0);
            tx.commit();
            
            tx.begin();
            log("Read node attribute: " + CHILD1_ATT_A_NAME);
            log("Value: " + child1.getAttribute(LANG_ORIG, CHILD1_ATT_A_NAME));
            tx.commit();
            tx.begin();
            log("Read node attribute: " + CHILD1_ATT_B_NAME);
            log("Value: " + child1.getAttribute(LANG_ORIG, CHILD1_ATT_B_NAME));
            tx.commit();
            tx.begin();
            log("Read node attribute: " + CHILD1_ATT_C_NAME);
            log("Value: " + child1.getAttribute(LANG_ORIG, CHILD1_ATT_C_NAME));
            tx.commit();
        } finally {
            sess.close();
        }
    }
    
    public static DbStore getDbStore(Session dbSession, String storeId)
    {
        Query q = dbSession.createQuery("from DbStore where storeDisplayId = :store_id");
        q.setString("store_id", storeId);
        return (DbStore) q.uniqueResult();
    }

    public static DbVersion getDbVersion(DbStore db_store, String verName)
    {
        Set v_set = db_store.allVersions();
        Iterator it = v_set.iterator();
        while (it.hasNext()) {
            DbVersion db_ver = (DbVersion) it.next();
            String vname = db_ver.getVersionName();
            try {
                if (vname.equals(verName)) {
                    return db_ver;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    private static String[] listVersionNames(DbStore db_store)
    {
        Set v_set = db_store.allVersions();
        ArrayList verIds = new ArrayList(v_set.size());
        Iterator it = v_set.iterator();
        while (it.hasNext()) {
            DbVersion db_version = (DbVersion) it.next();
            String vname = db_version.getVersionName();
            try {
                verIds.add(vname);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        Collections.sort(verIds);
        String[] verIdArr = new String[verIds.size()];
        return (String[]) verIds.toArray(verIdArr);
    }
    
    private static void log(String msg)
    {
        System.out.println("#### Test: " + msg);
    }
}
