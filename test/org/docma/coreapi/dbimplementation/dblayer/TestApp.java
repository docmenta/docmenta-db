/*
 * TestApp.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;

import org.hibernate.*;

import org.docma.util.Log;
import org.docma.coreapi.*;
import org.docma.hibernate.*;
import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;

/**
 *
 * @author MP
 */
public class TestApp
{ 

    /**
     * Before executing this method, the test database has to be initialized 
     * by executing the method CreateDDL.main(). Note that the database 
     * instance and the database user have to be manually created before. 
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        final String APP_PROP_NAME1 = "my.test.prop"; 
        final String APP_PROP_VALUE1 = "A test property value.";
        
        SessionFactory fact =
            HibernateUtil.getSessionFactory("com.mysql.jdbc.Driver",
                                            "jdbc:mysql://localhost:3306/docmentadb",
                                            "org.hibernate.dialect.MySQLDialect",
                                            "docma_usr",
                                            "docma_pw");

        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            //
            // Test reading and writing of application properties
            //
            tx = sess.beginTransaction();

            DbApplicationProperty app_prop = new DbApplicationProperty();
            app_prop.setPropName(APP_PROP_NAME1);
            app_prop.setPropValue(APP_PROP_VALUE1);

            sess.save(app_prop);   // saveOrUpdate(app_prop);
            tx.commit();
            tx = null;

            tx = sess.beginTransaction();
            List result = sess.createQuery("from DbApplicationProperty").list();
            tx.commit();
            tx = null;

            for (int i = 0; i < result.size(); i++) {
                DbApplicationProperty prop = (DbApplicationProperty) result.get(i);
                System.out.println("Property Name: " + prop.getPropName());
                System.out.println("Property Value: " + prop.getPropValue());
            }

            //
            // Test reading and writing of document store and versions
            //
            tx = sess.beginTransaction();

            DbStore store = new DbStore();
            store.setStoreDisplayId("myStoreName" + System.currentTimeMillis());
            sess.save(store);    // make persistent (connect store with session)

            DbVersion ver1 = new DbVersion();
            ver1.setVersionName("1.0.0");
            store.addVersion(ver1);
            System.out.println("Store assigned to ver1: " + ver1.getStore());
            sess.save(ver1);    // make persistent

            DbVersion ver2 = new DbVersion();
            ver2.setVersionName("Draft");
            ver2.setBaseVersion(ver1);
            store.addVersion(ver2);
            sess.save(ver2);    // make persistent

            tx.commit();
            tx = null;

            listStores(sess);

            //
            // Test deletion of version
            //
            tx = sess.beginTransaction();
            store.removeVersion(ver2);
            sess.delete(ver2);
            sess.flush();  // update(store);
            tx.commit();
            tx = null;

            listStores(sess);

            //
            // Test reading and writing of publication export attributes
            //
            tx = sess.beginTransaction();

            DbPublicationExport export = new DbPublicationExport();
            ver1.addPublicationExport(export);
            export.setExportName("publication" + System.currentTimeMillis());
            export.setExportSize(100);

            export.getAttributes().put("my.export.attname1", "test value 1");
            export.getAttributes().put("my.export.attname2", "test value 2");
            export.getAttributes().put("my.export.attname3", "test value 3");
            export.getAttributes().put("my.export.attname2", null);
//            export.setAttribute("my.export.attname1", "test value 1");
//            export.setAttribute("my.export.attname2", "test value 2");
//            export.setAttribute("my.export.attname3", "test value 3");
//            export.setAttribute("my.export.attname2", null);

            sess.save(export);   // saveOrUpdate(export);
            tx.commit();
            tx = null;

            listPubExportAttributes(sess, export.getPubExportDbId());

            tx = sess.beginTransaction();

            export.getAttributes().put("my.export.attname1", "changed test value 1");
            export.getAttributes().put("my.export.attname4", "test value 4");
            export.getAttributes().remove("my.export.attname3");

            sess.flush();  // update(export);
            tx.commit();
            tx = null;

            listPubExportAttributes(sess, export.getPubExportDbId());

            //
            // Test writing of nodes
            //
            tx = sess.beginTransaction();

            DbNode root1 = ver1.createNode();
            DbNode root2 = ver1.createNode();
            ver1.addRootNode(root1);
            ver1.addRootNode(root2);

            sess.flush();   // update(ver1);

            DbNode child1 = ver1.createNode();
            DbNode child2 = ver1.createNode();
            root1.addChildNode(child1);
            root1.addChildNode(child2);

            DbNode subchild1 = ver1.createNode();
            DbNode subchild2 = ver1.createNode();
            DbNode subchild3 = ver1.createNode();
            DbNode subchild4 = ver1.createNode();
            child1.addChildNode(subchild1);
            child1.addChildNode(subchild2);
            child2.addChildNode(subchild3);
            child2.addChildNode(subchild4);

            DbNode subsubchild1 = ver1.createNode();
            subchild1.addChildNode(subsubchild1);

            child1.setAttribute(LANG_ORIG, "child1_att_a", "1.attribute von child1");
            child1.setAttribute("EN", "child1_att_a", "1.attribute of child1");
            child1.setAttribute(LANG_ORIG, "child1_att_b", "2.attribute von child1");
            child1.setAttribute("EN", "child1_att_b", "2.attribute of child1");

            child2.setAttribute(LANG_ORIG, "child2_att_a", "1.attribute von child2");
            child2.setAttribute("EN", "child2_att_a", "1.attribute of child2");
            child2.setAttribute(LANG_ORIG, "child2_att_b", "2.attribute von child2");
            child2.setAttribute("EN", "child2_att_b", "2.attribute of child2");

            child1.appendAlias("alias_child1");
            child1.appendAlias("another_alias_child1");

            child2.appendAlias("alias_child2");
            child2.appendAlias("another_alias_child2");
            subchild3.appendAlias("an_alias_sub3");

            sess.flush();

            long currentTime = System.currentTimeMillis();
            child1.addLock("child1_checkout", "user_1", currentTime, 1000);
            child1.addLock("child1_otherlock", "user_1", currentTime, 2000);

            child2.addLock("child2_checkout", "user_2", currentTime, 3000);
            child2.addLock("child2_otherlock", "user_2", currentTime, 4000);

            sess.flush();   // saveOrUpdate(root1);

            tx.commit();
            tx = null;

            //
            // Test writing of binary content
            //
            tx = sess.beginTransaction();

            sess.update(child2);   // reconnect to session
            DbBinaryContent cont = child2.createBinaryContent(LANG_ORIG);
            DbBinaryContent cont_en = child2.createBinaryContent("EN");
            sess.save(cont);
            sess.save(cont_en);
            // sess.update(child2);

            byte[] buf = new byte[10];
            for (int i = 0; i < buf.length; i++) buf[i] = (byte) (2*i);
            java.sql.Blob blob = sess.getLobHelper().createBlob(buf);
            cont.setContent(blob);
            sess.update(cont);

            tx.commit();
            tx = null;

            //
            // Test update of nodes in new session
            //
            sess.close();
            sess = fact.openSession();
            tx = sess.beginTransaction();

            DbVersion myver =
              (DbVersion) sess.load(DbVersion.class, new Integer(ver1.getVersionDbId()));
            boolean is_new_instance = (myver != ver1);
            System.out.println("Loaded version is new instance: " + is_new_instance);

            DbNode myroot1 = (DbNode) myver.allRootNodes().get(0);
            DbNode myroot2 = (DbNode) myver.allRootNodes().get(1);
            System.out.println("Root is new instance: " + (myroot1 != root1));

            System.out.println("Count root nodes: " + myver.allRootNodes().size());
            System.out.println("myroot1: " + myroot1);
            System.out.println("myroot2: " + myroot2);
            System.out.println("Count children of 1.root: " + myroot1.allChildNodes().size());
            System.out.println("Count children of 2.root: " + myroot2.allChildNodes().size());

            myroot1.setNodeType("systemfolder");
            sess.flush();   // update(myroot1);

            DbNode mychild1 = (DbNode) myroot1.allChildNodes().get(0);
            DbNode mychild2 = (DbNode) myroot1.allChildNodes().get(1);
            System.out.println("mychild1: " + mychild1);
            System.out.println("mychild1.parent: " + mychild1.getParentNode());

            // remove 1. child:
            if (! myroot1.removeChildNode(mychild1)) {
                System.out.println("Could not remove child node.");
            }
            // myroot1.addChildNode(mychild1);
            sess.flush();


            // move 1. child at end of list:
            DbNode mysubchild3 = (DbNode) mychild2.allChildNodes().get(0);
            mychild2.removeChildNode(mysubchild3);
            mychild2.addChildNode(mysubchild3);
            sess.flush();

            // List all alias names
            List alias_list = sess.createQuery("select elements(node.aliases) from DbNode node").list();
            System.out.println("Alias count: " + alias_list.size());
            for (int i=0; i < alias_list.size(); i++) {
                Object obj = alias_list.get(i);
                System.out.println("Alias " + i + " (" + obj.getClass().getName() + "): " + obj);
            }

            tx.commit();
            tx = null;
        } catch (Exception he) {
            if (tx != null) tx.rollback();
            Log.error("" + he.getMessage());
            he.printStackTrace();
            throw new DocRuntimeException(he.getMessage());
        } finally {
            sess.close();
        }
    }


    public static void listStores(Session sess)
    {
        Transaction tx = sess.beginTransaction();
        List result = sess.createQuery("from DbStore").list();
        tx.commit();
        tx = null;

        for (int i = 0; i < result.size(); i++) {
            DbStore store = (DbStore) result.get(i);
            System.out.println(store);

            System.out.println("  Versions in Store:");
            Iterator it = store.allVersions().iterator();
            while (it.hasNext()) {
                DbVersion v = (DbVersion) it.next();
                System.out.println("    " + v);
            }
        }
    }


    public static void listPubExportAttributes(Session sess, int export_id)
    {
        Transaction tx = sess.beginTransaction();

        DbPublicationExport export =
            (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_id));
        System.out.println("Attributes of publication export with ID " + export_id + ":");
        Iterator it = export.getAttributes().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry att = (Map.Entry) it.next();
            System.out.println("    Name: " + att.getKey() + "  Value: " + att.getValue());
        }
//        Iterator it = export.allAttributes().iterator();
//        while (it.hasNext()) {
//            DbPubExportAttribute att = (DbPubExportAttribute) it.next();
//            System.out.println("    Name: " + att.getAttName() + "  Value: " + att.getAttValue());
//        }

        tx.commit();
        tx = null;
    }
}
