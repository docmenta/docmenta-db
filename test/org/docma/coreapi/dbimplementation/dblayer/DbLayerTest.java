/*
 * DbLayerTest.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

import org.junit.After;
import org.junit.Before;
// import org.junit.AfterClass;
// import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import static java.lang.System.*;
import java.util.*;
import java.io.*;
import org.hibernate.*;

import static org.docma.coreapi.dbimplementation.DbConstants.LANG_ORIG;
import org.docma.hibernate.*;
import org.docma.util.DocmaUtil;
import org.hibernate.criterion.Restrictions;

/**
 *
 * @author MP
 */
public class DbLayerTest 
{
    SessionFactory fact;
    
    public DbLayerTest() 
    {
    }
    
//    @BeforeClass
//    public static void setUpClass() 
//    {
//    }
//    
//    @AfterClass
//    public static void tearDownClass() 
//    {
//    }
    
    /**
     * This method initializes the test database. This method has to be 
     * executed before any test method. Note that the database 
     * instance and the database user have to be manually created before. 
     */
    @Before
    public void setUp() 
    {
        HibernateUtil.setExternalConnectionProperties(new File("C:\\work\\db_config.properties"));
        
        out.println("Executing setUp():");
        // setUp_MySQL_local();
        // setUp_MySQL_remote();
        // setUp_MySQL_remote2();
        setUp_Postgres_remote();
        // setUp_Postgres_remote2();
        // setUp_SQLServer_remote();
        // setUp_SQLServer_remote2();
        // setUp_Oracle_remote();
        // setUp_Oracle_remote2();
        // setUp_Derby();
        out.println("Method setUp() finished.");
    }
    
    private void setUp_Derby()
    {
        final String derby_system_path = "c:\\work\\derby_test_dbs\\derby_system";
        final String db_path = "c:/work/derby_test_dbs/db_unit_test/dbstore";
        
        final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
        final String CONNECTION_URL = "jdbc:derby:directory:" + db_path + ";create=true";
        final String SQL_DIALECT = "org.hibernate.dialect.DerbyDialect";
        final String DB_USER = null;
        final String DB_PASSWD = null;

        System.setProperty("derby.system.home", derby_system_path);

        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_MySQL_local()
    {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String CONNECTION_URL = "jdbc:mysql://localhost:3306/docmentadb";
        final String SQL_DIALECT = "org.hibernate.dialect.MySQLDialect";
        final String DB_USER = "docma_usr";
        final String DB_PASSWD = "docma_pw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_MySQL_remote()
    {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String CONNECTION_URL = "jdbc:mysql://qnap253:3306/docmatest";
        final String SQL_DIALECT = "org.hibernate.dialect.MySQLDialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_MySQL_remote2()
    {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String CONNECTION_URL = "jdbc:mysql://192.168.1.199:3306/docmentadb";
        final String SQL_DIALECT = "org.hibernate.dialect.MySQLDialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_Postgres_remote()
    {
        final String JDBC_DRIVER = "org.postgresql.Driver";
        final String CONNECTION_URL = "jdbc:postgresql://qnap253:5432/docmatest";
        final String SQL_DIALECT = "org.hibernate.dialect.PostgreSQLDialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_Postgres_remote2()
    {
        final String JDBC_DRIVER = "org.postgresql.Driver";
        final String CONNECTION_URL = "jdbc:postgresql://192.168.1.199:5432/DOCMENTADB";
        final String SQL_DIALECT = "org.hibernate.dialect.PostgreSQLDialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_SQLServer_remote()
    {
        final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        final String CONNECTION_URL = "jdbc:sqlserver://192.168.1.199:1433;databaseName=DOCMATEST;";
        final String SQL_DIALECT = "org.hibernate.dialect.SQLServer2008Dialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_SQLServer_remote2()
    {
        final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        final String CONNECTION_URL = "jdbc:sqlserver://192.168.1.199:1433;databaseName=DOCMENTADB;";
        final String SQL_DIALECT = "org.hibernate.dialect.SQLServer2008Dialect";
        final String DB_USER = "docmauser";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    private void setUp_Oracle_remote()
    {
        final String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
        final String CONNECTION_URL = "jdbc:oracle:thin:@192.168.1.199:1521";
        final String SQL_DIALECT = "org.hibernate.dialect.Oracle10gDialect";
        final String DB_USER = "docmatest";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }

    private void setUp_Oracle_remote2()
    {
        final String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
        final String CONNECTION_URL = "jdbc:oracle:thin:@192.168.1.199:1521";
        final String SQL_DIALECT = "org.hibernate.dialect.Oracle10gDialect";
        final String DB_USER = "docmentadb";
        final String DB_PASSWD = "docmapw";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        fact = HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }

    @After
    public void tearDown() 
    {
        out.println("Executing tearDown()");
    }

    
    //
    // Add test methods here. The methods must be annotated with annotation @Test.
    //

    
    @Test
    public void testDbLayer() throws Exception
    {
        out.println("========================");
        out.println("Executing testDbLayer():");
        out.println("========================");
        
        long time1 = System.currentTimeMillis();
        executeApplicationPropTests();
        long time2 = System.currentTimeMillis();
        executeStoreTests();
        long time3 = System.currentTimeMillis();
        executeDeleteStores();
        long time4 = System.currentTimeMillis();
        
        out.println("========================");
        out.println("Method testDbLayer() finished.");
        out.println("========================");
        out.println("Total time:          " + ((time4 - time1) / 1000.0) + " seconds.");
        out.println("App properties test: " + ((time2 - time1) / 1000.0) + " seconds.");
        out.println("Store tests:         " + ((time3 - time2) / 1000.0) + " seconds.");
        out.println("Delete tests:        " + ((time4 - time3) / 1000.0) + " seconds.");
        
    }

    private void executeStoreTests() throws Exception 
    {
        //
        // Test reading and writing of document store.
        //
        final String[] STORE_IDS = { "myStoreName1", "myStoreName2", "myStoreName3" };
        final String CHANGED_ID = "my_Changed_ID";
        
        final String PROP_NAME1 = "store.prop.1";
        final String PROP_NAME2 = "store.prop.2";
        final String PROP_NAME3 = "store.prop.3";
        final String PROP_NAME4 = "store.prop.4";
        final String PROP_NAME_ADDED = "store.prop.added";
        final String PROP_VAL1 = "Value 1 ";
        final String PROP_VAL2 = "Value 2 ";
        final String PROP_VAL3 = " Value 3 ";
        final String PROP_VAL4 = "  Value 4  ";
        final String PROP_VAL4_CHANGED = " Value4 changed! ";
        final String PROP_VAL_ADDED = "Value added";
        
        int expected_store_cnt = STORE_IDS.length;
        int expected_prop_cnt;
        
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            //
            // Write test data
            //
            tx = sess.beginTransaction();
            for (int i=0; i < STORE_IDS.length; i++) {
                DbStore store = new DbStore();
                String display_id = STORE_IDS[i];
                store.setStoreDisplayId(display_id);
                assertEquals("Store display ID is okay", display_id, store.getStoreDisplayId());
                
                Map store_props = store.getProperties();
                store_props.put(PROP_NAME1, display_id + ":" + PROP_VAL1);
                store_props.put(PROP_NAME2, display_id + ":" + PROP_VAL2);
                store_props.put(PROP_NAME3, display_id + ":" + PROP_VAL3);
                store_props.put(PROP_NAME4, display_id + ":" + PROP_VAL4);
                
                // Remove first property before persisting object 
                store_props.remove(PROP_NAME1);
                
                sess.save(store);    // make persistent (connect store with session)
            }
            tx.commit();
            tx = null;

            expected_prop_cnt = 3;

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();

            //
            // Test reading the stores.
            //
            tx = sess.beginTransaction();
            List result = sess.createQuery("from DbStore").list();
            assertTrue("Check store count", expected_store_cnt == result.size());
            ArrayList display_ids = new ArrayList();
            for (int i = 0; i < result.size(); i++) {
                DbStore store = (DbStore) result.get(i);
                display_ids.add(store.getStoreDisplayId());
            }
            List expected_ids = new ArrayList(Arrays.asList(STORE_IDS));
            Collections.sort(display_ids);
            Collections.sort(expected_ids);
            assertTrue("Check store display IDs", display_ids.equals(expected_ids));

            for (int i=0; i < STORE_IDS.length; i++) {
                Query query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
                query.setString("displayid", STORE_IDS[i]);
                DbStore store = (DbStore) query.uniqueResult();
                Map store_props = store.getProperties();
                assertTrue("Check store property count", store_props.size() == expected_prop_cnt);
                assertTrue(store_props.get(PROP_NAME1) == null);  // was removed
                assertEquals(store_props.get(PROP_NAME2), STORE_IDS[i] + ":" + PROP_VAL2);
                assertEquals(store_props.get(PROP_NAME3), STORE_IDS[i] + ":" + PROP_VAL3);
                assertEquals(store_props.get(PROP_NAME4), STORE_IDS[i] + ":" + PROP_VAL4);
            }
            tx.commit();
            tx = null;

            //
            // Test removing, adding and setting properties for the 1. store.
            //
            tx = sess.beginTransaction();
            Query query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", STORE_IDS[0]);
            DbStore store = (DbStore) query.uniqueResult();
            Map store_props = store.getProperties();
            store_props.remove(PROP_NAME3);                     // remove
            store_props.put(PROP_NAME_ADDED, PROP_VAL_ADDED);   // add
            store_props.put(PROP_NAME4, PROP_VAL4_CHANGED);      // change
            
            // Test changing the display ID of the 1. store.
            store.setStoreDisplayId(CHANGED_ID);
            
            tx.commit();
            tx = null;

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();

            // Reading the changed display ID and store properties
            tx = sess.beginTransaction();
            query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", CHANGED_ID);
            store = (DbStore) query.uniqueResult();
            store_props = store.getProperties();
            assertTrue("Store property count okay", store_props.size() == expected_prop_cnt);
            assertTrue(store_props.get(PROP_NAME3) == null);  // was removed
            assertEquals(store_props.get(PROP_NAME2), STORE_IDS[0] + ":" + PROP_VAL2);
            assertEquals(store_props.get(PROP_NAME_ADDED), PROP_VAL_ADDED);
            assertEquals(store_props.get(PROP_NAME4), PROP_VAL4_CHANGED);
            tx.commit();
            tx = null;
            
        } finally {
            sess.close();
        }
        
        executeVersionTests(CHANGED_ID);
    }

    private void executeDeleteStores() throws Exception 
    {
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            List result = sess.createQuery("from DbStore").list();
            for (Object obj : result) {
                sess.delete((DbStore) obj);
            }
            tx.commit();
        } finally {
            sess.close();
        }
        
        sess = fact.openSession();
        tx = null;
        try {
            tx = sess.beginTransaction();
            long cnt;
            // cnt = countDbObjects(sess, "DbApplicationProperty");
            // assertTrue("Table DbApplicationProperty is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbPublicationExport");
            assertTrue("Table DbPublicationExport is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbStore");
            assertTrue("Table DbStore is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbVersion");
            assertTrue("Table DbVersion is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbNode");
            assertTrue("Table DbNode is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbNodeLock");
            assertTrue("Table DbNodeLock is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbBinaryContent");
            assertTrue("Table DbBinaryContent is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbBinaryLob");
            assertTrue("Table DbBinaryLob is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbTextContent");
            assertTrue("Table DbTextContent is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbImageRendition");
            assertTrue("Table DbImageRendition is empty", cnt == 0);
            
            cnt = countDbObjects(sess, "DbAlias");
            assertTrue("Table DbAlias is empty", cnt == 0);
            
            tx.commit();
        } finally {
            sess.close();
        }
    }
    
    private long countDbObjects(Session sess, String objName)
    {
        // String alias = objName.toLowerCase();
        // Query q = sess.createQuery("select count(" + alias + ") from " + objName + " " + alias);
        Query q = sess.createQuery("select count(*) from " + objName);
        return ((Number) q.uniqueResult()).longValue();
    }


    private void executeVersionTests(String storeId) throws Exception 
    {
        //
        // Test reading and writing of store versions.
        //
        final String VER1_NAME = "1.0.0";
        final String VER1_NAME_CHANGED = "1.2";
        final String VER2_NAME = "Draft";
        
        final String PROP_NAME1 = "ver.prop.1";
        final String PROP_NAME2 = "ver.prop.2";
        final String PROP_NAME3 = "ver.prop.3";
        final String PROP_NAME4 = "ver.prop.4";
        // final String PROP_NAME5 = "ver.prop.5";
        final String PROP_NAME_ADDED = "ver.prop.added";
        final String PROP_VAL1 = "Value 1 ";
        final String PROP_VAL2 = "Value 2 ";
        final String PROP_VAL3 = " Value 3 ";
        final String PROP_VAL4 = "  Value 4  ";
        final String PROP_VAL4_CHANGED = " Value4 changed! ";
        final String PROP_VAL_ADDED = "Value added";
        // final String PROP_VAL5_CHANGED = " Value5 changed! ";

        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            //
            // Write test data: create versions, create version relation, set version name and properties.
            //
            tx = sess.beginTransaction();
            Query query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", storeId);
            DbStore store = (DbStore) query.uniqueResult();

            DbVersion ver1 = new DbVersion();
            ver1.setVersionName(VER1_NAME);
            store.addVersion(ver1);
            assertEquals("Check version name assignment", VER1_NAME, ver1.getVersionName());
            assertSame("Check store to version assignment", store, ver1.getStore());

            Map ver1_props = ver1.getProperties();
            ver1_props.put(PROP_NAME1, VER1_NAME + ":" + PROP_VAL1);
            ver1_props.put(PROP_NAME2, VER1_NAME + ":" + PROP_VAL2);
            ver1_props.put(PROP_NAME3, VER1_NAME + ":" + PROP_VAL3);
            ver1_props.put(PROP_NAME4, VER1_NAME + ":" + PROP_VAL4);
            // ver1_props.put(PROP_NAME5, "");   // Test empty string value (Note: Oracle converts empty string to null! I love it!)

            // Remove first property before persisting object 
            ver1_props.remove(PROP_NAME1);

            sess.save(ver1);    // make persistent

            DbVersion ver2 = new DbVersion();
            ver2.setVersionName(VER2_NAME);
            ver2.setBaseVersion(ver1);
            store.addVersion(ver2);
            assertSame("Check base version assignment", ver1, ver2.getBaseVersion());
            assertTrue("Check version count", store.allVersions().size() == 2);
            assertTrue("Check 1. version to store assignment", store.allVersions().contains(ver1));
            assertTrue("Check 2. version to store assignment", store.allVersions().contains(ver2));
            
            Map ver2_props = ver2.getProperties();
            ver2_props.put(PROP_NAME1, VER2_NAME + ":" + PROP_VAL1);
            ver2_props.put(PROP_NAME2, VER2_NAME + ":" + PROP_VAL2);
            ver2_props.put(PROP_NAME3, VER2_NAME + ":" + PROP_VAL3);
            ver2_props.put(PROP_NAME4, VER2_NAME + ":" + PROP_VAL4);
            
            sess.save(ver2);    // make persistent

            tx.commit();
            tx = null;

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            //
            // Read test data
            // 
            tx = sess.beginTransaction();
            query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", storeId);
            store = (DbStore) query.uniqueResult();
            
            assertTrue("Check version count", store.allVersions().size() == 2);

            ver2 = getVersionByName(store, VER2_NAME);
            assertTrue("Check reading 1. version", ver2 != null);
            ver1 = ver2.getBaseVersion();
            assertSame("Check reading store to 1. version assignment", store, ver1.getStore());
            assertSame("Check reading store to 2. version assignment", store, ver2.getStore());
            assertEquals("Check version name assignment", VER1_NAME, ver1.getVersionName());
            ver1_props = ver1.getProperties();
            ver2_props = ver2.getProperties();
            // Note: Oracle transforms empty string to null. 
            // In this case PROP_NAME5 is not in the list, though a row exists for this property!
            // int expected_ver1_prop_count = ver1_props.containsKey(PROP_NAME5) ? 4 : 3;  // written 5 properties from which one was deleted
            int expected_ver1_prop_count = 3;  // written 4 properties from which one was deleted
            int expected_ver2_prop_count = 4;  // written 4 properties
            assertTrue("Check property count of 1. version", ver1_props.size() == expected_ver1_prop_count);
            assertTrue("Check property count of 2. version", ver2_props.size() == expected_ver2_prop_count);

            // Test reading properties of 1. version
            assertTrue(ver1_props.get(PROP_NAME1) == null);  // was removed
            assertEquals(ver1_props.get(PROP_NAME2), VER1_NAME + ":" + PROP_VAL2);
            assertEquals(ver1_props.get(PROP_NAME3), VER1_NAME + ":" + PROP_VAL3);
            assertEquals(ver1_props.get(PROP_NAME4), VER1_NAME + ":" + PROP_VAL4);
            
            // Test reading properties of 2. version
            assertEquals(ver2_props.get(PROP_NAME1), VER2_NAME + ":" + PROP_VAL1);
            assertEquals(ver2_props.get(PROP_NAME2), VER2_NAME + ":" + PROP_VAL2);
            assertEquals(ver2_props.get(PROP_NAME3), VER2_NAME + ":" + PROP_VAL3);
            assertEquals(ver2_props.get(PROP_NAME4), VER2_NAME + ":" + PROP_VAL4);

            //
            // Update version name and properties
            //
            ver1.setVersionName(VER1_NAME_CHANGED);
            ver1_props.remove(PROP_NAME3);                     // remove
            ver1_props.put(PROP_NAME_ADDED, PROP_VAL_ADDED);   // add
            ver1_props.put(PROP_NAME4, PROP_VAL4_CHANGED);     // change
            // ver1_props.put(PROP_NAME5, PROP_VAL5_CHANGED);     // change empty string to non-empty string
            // expected_ver1_prop_count = 4; // one removed, one added, empty string value no longer exists

            tx.commit();
            tx = null;

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            //
            // Read updated version name and properties
            //
            tx = sess.beginTransaction();
            sess.load(store, store.getStoreDbId());
            ver2 = getVersionByName(store, VER2_NAME);
            ver1 = ver2.getBaseVersion();
            assertEquals("Check updated version name", VER1_NAME_CHANGED, ver1.getVersionName());
            ver1_props = ver1.getProperties();
            assertTrue("Check version property count after update", ver1_props.size() == expected_ver1_prop_count);
            // Test reading properties of 1. version
            assertTrue(ver1_props.get(PROP_NAME1) == null);  // was removed
            assertEquals(ver1_props.get(PROP_NAME2), VER1_NAME + ":" + PROP_VAL2);
            assertTrue(ver1_props.get(PROP_NAME3) == null);  // was removed
            assertEquals(ver1_props.get(PROP_NAME_ADDED), PROP_VAL_ADDED);
            assertEquals(ver1_props.get(PROP_NAME4), PROP_VAL4_CHANGED);
            
            //
            // Test deletion of version
            //
            store.removeVersion(ver2);
            sess.delete(ver2);
            // sess.flush();  // update(store);
            tx.commit();
            tx = null;
            
            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            tx = sess.beginTransaction();
            sess.load(store, store.getStoreDbId());
            assertTrue("Check version count after deletion", store.allVersions().size() == 1);
            ver1 = (DbVersion) store.allVersions().iterator().next();
            ver1_props = ver1.getProperties();
            assertEquals("Check version name after deletion", VER1_NAME_CHANGED, ver1.getVersionName());
            assertTrue("Check version property count after deletion", ver1_props.size() == expected_ver1_prop_count);
            
            tx.commit();
            tx = null;
        } finally {
            sess.close();
        }
        
        executePublicationExportTests(storeId, VER1_NAME_CHANGED);
        executeNodeTests(storeId, VER1_NAME_CHANGED);
    }
    
    
    private void executePublicationExportTests(String storeId, String verId) throws Exception 
    {
        final String EXPORT1_NAME = "publication1";
        final String EXPORT1_NAME_CHANGED = "publication1x";
        final int EXPORT1_SIZE = 100;
        final int EXPORT1_SIZE_CHANGED = 200;

        final String ATT_NAME1 = "attname.1";
        final String ATT_NAME2 = "attname.2";
        final String ATT_NAME3 = "attname.3";
        final String ATT_NAME_ADDED = "attname.added";
        final String ATT_VALUE1 = "publication attribute value 1";
        final String ATT_VALUE1_CHANGED = " publication attribute value changed ";
        final String ATT_VALUE2 = "publication attribute value 2";
        final String ATT_VALUE3 = "publication attribute value 3";
        final String ATT_VALUE_ADDED = " publication attribute value added ";
        
        //
        // Test reading and writing of publication export data.
        //
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            //
            // Test reading and writing of publication export attributes
            //
            tx = sess.beginTransaction();
            Query query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", storeId);
            DbStore store = (DbStore) query.uniqueResult();
            DbVersion ver = getVersionByName(store, verId);
            assertTrue(ver != null);

            DbPublicationExport export = new DbPublicationExport();
            ver.addPublicationExport(export);
            export.setExportName(EXPORT1_NAME);
            export.setExportSize(EXPORT1_SIZE);

            Map exp_atts = export.getAttributes();
            exp_atts.put(ATT_NAME1, ATT_VALUE1);
            exp_atts.put(ATT_NAME2, ATT_VALUE2);
            exp_atts.put(ATT_NAME3, ATT_VALUE3);
            exp_atts.put(ATT_NAME2, null);

//            export.setAttribute("my.export.attname1", "test value 1");
//            export.setAttribute("my.export.attname2", "test value 2");
//            export.setAttribute("my.export.attname3", "test value 3");
//            export.setAttribute("my.export.attname2", null);

            sess.save(export);   // saveOrUpdate(export);
            tx.commit();
            tx = null;

            int export_db_id = export.getPubExportDbId();
            
            int expected_atts_count = 2;  // or 3 because of null value?

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            tx = sess.beginTransaction();
            export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));
            assertEquals("Check publiction export name", EXPORT1_NAME, export.getExportName());
            assertTrue("Check publication export size", EXPORT1_SIZE == export.getExportSize());
            exp_atts = export.getAttributes();
            assertTrue("Check export attributes count", exp_atts.size() == expected_atts_count);
            assertEquals(exp_atts.get(ATT_NAME1), ATT_VALUE1);
            assertTrue(exp_atts.get(ATT_NAME2) == null);
            assertEquals(exp_atts.get(ATT_NAME3), ATT_VALUE3);

            //
            // Update name, size and attributes
            //
            export.setExportName(EXPORT1_NAME_CHANGED);
            export.setExportSize(EXPORT1_SIZE_CHANGED);
            exp_atts.put(ATT_NAME1, ATT_VALUE1_CHANGED);
            exp_atts.put(ATT_NAME2, ATT_VALUE2);
            exp_atts.put(ATT_NAME_ADDED, ATT_VALUE_ADDED);
            exp_atts.remove(ATT_NAME3);

            // sess.flush();  // update(export);
            tx.commit();
            tx = null;

            expected_atts_count = 3; 

            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            //
            // Read updated attributes
            //
            tx = sess.beginTransaction();
            export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));
            assertEquals("Check updated export name", EXPORT1_NAME_CHANGED, export.getExportName());
            assertTrue("Check updated export size", EXPORT1_SIZE_CHANGED == export.getExportSize());
            exp_atts = export.getAttributes();
            assertTrue("Check export attributes count", exp_atts.size() == expected_atts_count);
            assertEquals(exp_atts.get(ATT_NAME1), ATT_VALUE1_CHANGED);
            assertEquals(exp_atts.get(ATT_NAME2), ATT_VALUE2);
            assertTrue(exp_atts.get(ATT_NAME3) == null);
            assertEquals(exp_atts.get(ATT_NAME_ADDED), ATT_VALUE_ADDED);

            // tx.commit();
            // tx = null;
            
            //
            // Write publication stream
            //
            // tx = sess.beginTransaction();
            // export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));

            // byte[] testcontent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
            // File fpath = new File("C:\\TEMP\\reference_manual_1-6_pdf.pdf");
            // FileInputStream fin = new FileInputStream(fpath);
            int blob_length = 2*1024*1024;  // 2 MB   // fpath.length();
            
            // setBlobFromStream(sess, new ByteArrayInputStream(testcontent), export);
            java.sql.Blob blob = export.getExportFile();
            assertTrue(blob == null);
            blob = createNewExportBlob(sess, export);
            writeTestBlobStream(blob, blob_length, (byte) 1);
//            OutputStream outStream = blob.setBinaryStream(1);  // according to api documentation,
//            try {
//                DocmaUtil.copyStream(fin, outStream); // outStream.write(testcontent);
//            } finally {
//                // outStream.flush();
//                outStream.close();
//                fin.close();
//            }
//            if (blob.length() > fpath.length()) { 
//                blob.truncate(fpath.length());
//            }

            sess.flush();   // export.saveExportFileBlob(sess); 
            // export.refreshExportFileBlob(sess);  // read persisted blob from DB
            // blob = export.getExportFile();  // get persisted blob 
            
            long len = export.getExportFile().length();
            assertEquals(blob_length, len);
            export.setExportSize(len);
            String now_millis = Long.toString(System.currentTimeMillis());
            export.getAttributes().put("closing_time", now_millis);
            
            tx.commit();
            tx = null;
            
            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            //
            // Read publication stream from new session
            //
            tx = sess.beginTransaction();
            export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));
            blob = export.getExportFile();
            len = blob.length();
            assertEquals(blob_length, len);
            assertTrue(testInputStreamMatches(blob.getBinaryStream(), blob_length, (byte) 1));
            
            tx.commit();
            tx = null;
            
            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();

            //
            // Overwrite publication stream with shorter stream
            //
            tx = sess.beginTransaction();
            
            export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));
            blob = createNewExportBlob(sess, export);  // export.getExportFile();
            int blob_length_shorter = 1024*1024;
            writeTestBlobStream(blob, blob_length_shorter, (byte) 7);
            sess.flush();   // export.saveExportFileBlob(sess); 
            
            tx.commit();
            tx = null;
            // Close session and open new session to avoid reading from cache.
            sess.close();
            sess = fact.openSession();
            
            //
            // Read modified publication stream from new session
            //
            tx = sess.beginTransaction();
            export = (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_db_id));
            blob = export.getExportFile();
            len = blob.length();
            System.out.println("Actual blob length: " + len);
            assertEquals(blob_length_shorter, len);
            assertTrue(testInputStreamMatches(blob.getBinaryStream(), blob_length_shorter, (byte) 7));
            
            tx.commit();
            tx = null;
        } finally {
            sess.close();
        }
    }
    
    private java.sql.Blob createNewExportBlob(Session sess, DbPublicationExport export)
    {
        final byte[] dummy_arr = {};
        java.sql.Blob blob = sess.getLobHelper().createBlob(dummy_arr); // sess.getLobHelper().createBlob(fin, fpath.length()); 
        export.setExportFile(blob);
        // Following is a workaround, because the Blob created by 
        // Hibernate LobHelper does not support all stream operations 
        // (i.e. otherwise following exception is thrown:
        // java.lang.UnsupportedOperationException: Blob may not be manipulated from creating session) 
        System.out.println("Blob class before: " + blob.getClass().getName());
        if (blob.getClass().getName().contains("$Proxy")) {
            System.out.println("Flush.");
            sess.flush();       // make blob persistent
            System.out.println("Refresh.");
            export.refreshExportFileBlob(sess);  // read persisted blob from DB
            blob = export.getExportFile();  // get persisted blob 
        }
        System.out.println("Blob class after: " + blob.getClass().getName());
        // Clear Blob content 
        // if (blob.length() > 0) blob.truncate(0);
        return blob;
    }
    
    private java.sql.Blob createNewContentBlob(Session sess, DbBinaryContent content)
    {
        final byte[] dummy_arr = {};
        java.sql.Blob blob = sess.getLobHelper().createBlob(dummy_arr); // sess.getLobHelper().createBlob(fin, fpath.length()); 
        content.setContent(blob);
        // Following is a workaround, because the Blob created by 
        // Hibernate LobHelper does not support all stream operations 
        // (i.e. otherwise following exception is thrown:
        // java.lang.UnsupportedOperationException: Blob may not be manipulated from creating session) 
        System.out.println("Blob class before: " + blob.getClass().getName());
        if (blob.getClass().getName().contains("$Proxy")) {
            DbBinaryLob bin_lob = (DbBinaryLob) content.getLobWrapper().get(0);
            System.out.println("Lob DB ID persisted: " + bin_lob.persisted());
            System.out.println("Lob DB ID before flush: " + bin_lob.getLobDbId());
            sess.flush();       // make blob persistent
            System.out.println("Lob DB ID persisted: " + bin_lob.persisted());
            bin_lob = (DbBinaryLob) content.getLobWrapper().get(0);
            System.out.println("Lob DB ID after flush: " + bin_lob.getLobDbId());
            sess.refresh(bin_lob);  // read persisted blob from DB
            bin_lob = (DbBinaryLob) content.getLobWrapper().get(0);
            System.out.println("Lob DB ID after refresh: " + bin_lob.getLobDbId());
            blob = content.getContent();  // get persisted blob 
        }
        System.out.println("Blob class after: " + blob.getClass().getName());
        // Clear Blob content 
        // if (blob.length() > 0) blob.truncate(0);
        return blob;
    }
    
    private void writeTestBlobStream(java.sql.Blob blob, int blob_length, byte start_value) throws Exception
    {
        InputStream in = createTestInputStream(blob_length, start_value);
        OutputStream outStream = blob.setBinaryStream(1);  // according to api documentation,
        try {
            DocmaUtil.copyStream(in, outStream); // outStream.write(testcontent);
        } finally {
            // outStream.flush();
            outStream.close();
            in.close();
        }
        long new_len = blob.length();
        System.out.println("Blob length after writing " + blob_length + " bytes: " + new_len);
        if (new_len > blob_length) {
            System.out.println("Blob needs to be truncated!");
            blob.truncate(blob_length);
        }
    }
    
    private InputStream createTestInputStream(int len, byte start_value) throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream(len);
        byte val = start_value;
        for (int i = 0; i < len; i++) {
            out.write(val);
            ++val;
        }
        out.close();
        return new ByteArrayInputStream(out.toByteArray());
    }
    
    private boolean testInputStreamMatches(InputStream in, int len, byte start_value) throws Exception
    {
        boolean matches = true;
        byte b = start_value;
        int cnt = 0;
        int val;
        while ((val = in.read()) >= 0) {
            if (++cnt > len) {
                // System.out.println("Blob size exceeds expected size of " + len + " bytes");
                matches = false;
                break;
            }
            if ((byte) val != b) {
                System.out.println("Blob mismatch. Expected byte: " + b + ". Actual byte: " + (byte) val);
                matches = false;
                break;
            }
            ++b;
        }
        if (cnt != len) {   // EOF has been reached before len (cnt < len)
            if (cnt > len) {
                System.out.println("Blob exceeds expected size of " + len + " bytes.");
            } else {
                System.out.println("Blob too small. Expected " + len + " bytes. Actual length: " + cnt);
            }
            matches = false;
        }
        return matches;
    }

    private void setBlobFromStream(Session sess, InputStream instream, DbPublicationExport export)
    throws Exception
    {
        final int DEFAULT_BUF_SIZE = 8192;  // 8 KB
        final int MAX_BUF_SIZE = 2621440;   // 2.5 MB
        
        // Read available bytes into buffer startbuf
        int avail = instream.available();
        final int startlen = ((avail > MAX_BUF_SIZE) || (avail == 0)) ? DEFAULT_BUF_SIZE : avail;
        byte[] startbuf = new byte[startlen];
        int cnt;
        do { 
            cnt = instream.read(startbuf); 
        } while (cnt == 0);
        boolean end_reached = (cnt < 0);
        long total_length = (cnt > 0) ? cnt : 0;
        
        java.sql.Blob lob = export.getExportFile();  // this method always returns a persisted Blob!
        byte[] buf = null;
        int offset = 1; // 1st byte in Blob has position 1
        if (lob == null) {   // New Blob object has to be created
            if (cnt > 0) {
                if (cnt < startbuf.length) {
                    startbuf = Arrays.copyOf(startbuf, cnt);   // trim buffer
                }
                lob = sess.getLobHelper().createBlob(startbuf);
                export.setExportFile(lob);
                sess.flush();       // make blob persistent
                export.refreshExportFileBlob(sess);  // read persisted blob from DB
                // blob = export.getExportFile();  // get persisted blob 
                
                int nextbyte = instream.read();
                if (nextbyte < 0) {  // no more bytes
                    cnt = -1;
                    end_reached = true;
                } else {   // further bytes exist that have to be appended
                    buf = new byte[DEFAULT_BUF_SIZE];
                    buf[0] = (byte) nextbyte;
                    int c = instream.read(buf, 1, buf.length - 1);  // try to fill buffer
                    if (c < 0) {
                        cnt = 1;      // amount of bytes in buf
                        end_reached = true;
                    } else {
                        cnt = 1 + c;  // amount of bytes in buf
                    }
                    total_length += cnt;             // amount of bytes read from input stream
                    offset = startbuf.length + 1;    // append position (1st byte in Blob has position 1)
                    lob = export.getExportFile();  // retrieve persisted(!) blob because bytes have to be appended
                }
            }
        } else {  // Existing Blob has to be overwritten
            buf = startbuf;
        }
        
        // Existing Blob has to be overwritten or bytes have to be appended to newly created Blob.
        if (cnt > 0) {   // cnt bytes in buf have to be written
            OutputStream out = lob.setBinaryStream(offset);
            try {
                out.write(buf, 0, cnt);
                if (! end_reached) {
                    if (buf.length < 4096) buf = new byte[DEFAULT_BUF_SIZE];
                    while ((cnt = instream.read(buf)) >= 0) {
                        total_length += cnt;
                        out.write(buf, 0, cnt);
                    }
                }
            } finally {
                out.close();
            }
        }
        if ((lob != null) && (total_length < lob.length())) {
            lob.truncate(total_length);
        }
        export.setExportSize(total_length);
    }


    private void executeNodeTests(String storeId, String verId) throws Exception 
    {
        final String USER_ID1 = "user_1";
        final String USER_ID2 = "user_2";
        final String LOCK_NAME = "checkout";
        final String LOCK_NAME_ADDED = "checkout_added";
        final String CHILD1_ALIAS1 = "alias_child1";
        final String CHILD1_ALIAS1_CHANGED = "changed_alias_child1";
        final String CHILD1_ALIAS2 = "another_alias_child1";
        final String CHILD1_LOCK_NAME = "child1_lock_name";
        final String CHILD1_ATT_A_NAME = "child1_att_a";
        final String CHILD1_ATT_A_VALUE = "1. attribute of child1";
        final String CHILD1_ATT_A_VALUE_TRANSLATED = "1. translated attribute of child1";
        final String CHILD1_ATT_B_NAME = "child1_att_b";
        final String CHILD1_ATT_B_VALUE = "2. attribute of child1";
        final String CHILD1_ATT_B_VALUE_TRANSLATED = "2. translated attribute of child1";

        final String CHILD2_ALIAS1 = "alias_child2";
        final String CHILD2_ALIAS2 = "another_alias_child2";
        final String CHILD2_ALIAS_ADDED = "added_alias_child2";
        final String CHILD2_LOCK_NAME = "child2_lock_name";
        final String CHILD2_ATT_A_NAME = "child2_att_a";
        final String CHILD2_ATT_A_VALUE = "1. attribute of child2";
        final String CHILD2_ATT_A_VALUE_TRANSLATED = "1. translated attribute of child2";
        final String CHILD2_ATT_A_VALUE_CHANGED = "1. changed attribute of child2";
        final String CHILD2_ATT_B_NAME = "child2_att_b";
        final String CHILD2_ATT_B_VALUE = "2. attribute of child2";
        final String CHILD2_ATT_B_VALUE_TRANSLATED = "2. translated attribute of child2";
        
        final String ROOT1_ATT_NAME = "root1_att";
        final String ROOT1_ATT_VALUE = "Attribute value of root1";
        
        final String SUBCHILD2_ALIAS1 = "an_alias1_sub2";
        final String SUBCHILD2_ALIAS2 = "an_alias2_sub2";
        final String SUBCHILD2_ALIAS3 = "an_alias3_sub2";
        final String SUBCHILD3_ALIAS = "an_alias_sub3";

        final String SUBSUBCHILD1_ALIAS = "an_alias_subsub1";

        final String NODE_TYPE_IMAGE = "image";
        
        final String TXT_CONTENT = "A text content.";
        final String TXT_CONTENT_CHANGED = "Changed txt";
        final String TXT_CONTENT_TRANSLATED = "A translated text content.";
        
        byte[] BINARY_CONTENT = new byte[10];
        byte[] BINARY_CONTENT_TRANSLATED = new byte[20];
        for (int i = 0; i < BINARY_CONTENT.length; i++) { 
            BINARY_CONTENT[i] = (byte) (2*i);
        }
        for (int i = 0; i < BINARY_CONTENT_TRANSLATED.length; i++) {
            BINARY_CONTENT_TRANSLATED[i] = (byte) (3*i);
        }

        //
        // Test reading and writing of nodes.
        //
        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            System.out.println("------");
            System.out.println("Test writing of nodes:");
            System.out.println("------");
            //
            // Test writing of nodes
            //
            tx = sess.beginTransaction();
            System.out.println("---- Getting store and version object: ----");
            Query query = sess.createQuery("from DbStore where storeDisplayId = :displayid");
            query.setString("displayid", storeId);
            DbStore store = (DbStore) query.uniqueResult();
            DbVersion ver = getVersionByName(store, verId);
            assertTrue(ver != null);
            int ver_db_id = ver.getVersionDbId();
        
            System.out.println("---- Creating root nodes: ----");
            DbNode root1 = ver.createNode();
            DbNode root2 = ver.createNode();
            ver.addRootNode(root1);
            ver.addRootNode(root2);

            System.out.println("---- Flushing ----");
            sess.flush();   // update(ver1);

            System.out.println("---- Creating child nodes: ----");
            DbNode child1 = ver.createNode();
            DbNode child2 = ver.createNode();
            root1.addChildNode(child1);
            root1.addChildNode(child2);
            long child1_number = child1.getNodeNumber();
            long child2_number = child2.getNodeNumber();

            DbNode subchild1 = ver.createNode();
            DbNode subchild2 = ver.createNode();
            DbNode subchild3 = ver.createNode();
            // DbNode subchild4 = ver.createNode();
            child1.addChildNode(subchild1);
            child1.addChildNode(subchild2);
            child2.addChildNode(subchild3);
            // child2.addChildNode(subchild4);

            DbNode subsubchild1 = ver.createNode();
            subchild1.addChildNode(subsubchild1);
            
            /*
             * root1
             *   +- child1
             *       +- subchild1
             *            +- subsubchild1 
             *       +- subchild2
             *   +- child2
             *       +- subchild3
             * 
             * root2
             * 
             */

            System.out.println("---- Setting attributes: ----");
            //
            // Set attributes
            //
            child1.setAttribute(LANG_ORIG,   CHILD1_ATT_A_NAME, CHILD1_ATT_A_VALUE);
            child1.setAttribute("EN", CHILD1_ATT_A_NAME, CHILD1_ATT_A_VALUE_TRANSLATED);
            child1.setAttribute(LANG_ORIG,   CHILD1_ATT_B_NAME, CHILD1_ATT_B_VALUE);
            child1.setAttribute("EN", CHILD1_ATT_B_NAME, CHILD1_ATT_B_VALUE_TRANSLATED);

            child2.setAttribute(LANG_ORIG,   CHILD2_ATT_A_NAME, CHILD2_ATT_A_VALUE);
            child2.setAttribute("EN", CHILD2_ATT_A_NAME, CHILD2_ATT_A_VALUE_TRANSLATED);
            child2.setAttribute(LANG_ORIG,   CHILD2_ATT_B_NAME, CHILD2_ATT_B_VALUE);
            child2.setAttribute("EN", CHILD2_ATT_B_NAME, CHILD2_ATT_B_VALUE_TRANSLATED);

            System.out.println("---- Setting aliases: ----");
            //
            // Set aliases
            //
            child1.appendAlias(CHILD1_ALIAS1);
            child1.appendAlias(CHILD1_ALIAS2);
            child2.appendAlias(CHILD2_ALIAS1);
            child2.appendAlias(CHILD2_ALIAS2);
            subchild2.appendAlias(SUBCHILD2_ALIAS1);
            subchild2.appendAlias(SUBCHILD2_ALIAS2);
            subchild2.appendAlias(SUBCHILD2_ALIAS3);
            subchild3.appendAlias(SUBCHILD3_ALIAS);

            System.out.println("---- Flushing ----");
            sess.flush();

            System.out.println("---- Setting locks: ----");
            //
            // Set locks
            //
            long currentTime = System.currentTimeMillis();
            child1.addLock(LOCK_NAME, USER_ID1, currentTime, 1000);
            child1.addLock(CHILD1_LOCK_NAME, USER_ID1, currentTime, 2000);
            
            child2.addLock(LOCK_NAME, USER_ID1, currentTime, 3000);
            child2.addLock(CHILD2_LOCK_NAME, USER_ID2, currentTime, 4000);

            System.out.println("---- Flushing ----");
            sess.flush();   // saveOrUpdate(root1);

            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;

            //
            // Write text content (original and translated)
            //
            tx = sess.beginTransaction();
            
            System.out.println("---- Setting text content (original): ----");
            sess.update(subchild1);   // reconnect to session
            DbTextContent txt = subchild1.createTextContent(LANG_ORIG);
            // java.sql.Clob newclob = sess.getLobHelper().createClob("");
            // txt.setContent(newclob);
            txt.setContentType("txt/html");
            sess.save(txt);
            writeTextContent(TXT_CONTENT, txt);
            
            System.out.println("---- Setting text content (translation EN): ----");
            DbTextContent txt_en = subchild1.createTextContent("EN");
            // java.sql.Clob newclob_en = sess.getLobHelper().createClob("");
            // txt_en.setContent(newclob_en);
            sess.save(txt_en);
            writeTextContent(TXT_CONTENT_TRANSLATED, txt_en);
            
            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;

            //
            // Write binary content (original and translated)
            //
            tx = sess.beginTransaction();

            System.out.println("---- Setting binary content (original): ----");
            sess.update(child2);   // reconnect to session
            child2.setNodeType(NODE_TYPE_IMAGE);
            DbBinaryContent cont = child2.createBinaryContent(LANG_ORIG);
            DbBinaryContent cont_en = child2.createBinaryContent("EN");
            cont.setContentType("image/jpeg");
            // sess.save(cont.getLobWrapper());
            // sess.save(cont);
            // sess.save(cont_en.getLobWrapper());
            // sess.save(cont_en);
            // // sess.update(child2);

            java.sql.Blob blob = sess.getLobHelper().createBlob(BINARY_CONTENT);
            // java.sql.Blob blob = sess.getLobHelper().createBlob(new byte[0]);
            cont.setContent(blob);
            cont.setContentLength(BINARY_CONTENT.length);
            // sess.update(cont);
            
            System.out.println("---- Setting binary content (translation EN): ----");
            java.sql.Blob blob_en = sess.getLobHelper().createBlob(BINARY_CONTENT_TRANSLATED);
            cont_en.setContent(blob_en);
            cont_en.setContentLength(BINARY_CONTENT_TRANSLATED.length);
            // sess.update(cont_en);
            
            System.out.println("---- Commit ----");
            tx.commit();
            tx = sess.beginTransaction();
            
            System.out.println("---- Setting binary content stream: ----");
            sess.update(subsubchild1);   // reconnect to session
            subsubchild1.setNodeType(NODE_TYPE_IMAGE);
            subsubchild1.appendAlias(SUBSUBCHILD1_ALIAS);
            DbBinaryContent cont2 = subsubchild1.createBinaryContent(LANG_ORIG);
            cont2.setContentType("image/jpeg");
            java.sql.Blob blob2 = createNewContentBlob(sess, cont2);
            writeTestBlobStream(blob2, 1024*1024, (byte) 5);

            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;

            //
            // Test update Blob in other session/transaction
            //
//            sess.close();
//            System.out.println("---- Opening new session ----");
//            sess = fact.openSession();
//            tx = sess.beginTransaction();
//            sess.update(child2);   // reconnect to session
//            cont = child2.getBinaryContent(LANG_ORIG);
//            // sess.refresh(cont);
//            blob = cont.getContent();
//            OutputStream out = blob.setBinaryStream(1);
//            InputStream instream = new ByteArrayInputStream(BINARY_CONTENT);
//            long total_length = 0;
//            byte[] buf = new byte[8*1024];
//            int cnt;
//            while ((cnt = instream.read(buf)) >= 0) {
//                out.write(buf, 0, cnt);
//                total_length += cnt;
//            }
//            out.close();
//            if (total_length < blob.length()) {
//                blob.truncate(total_length);
//            }
//            cont.setContentLength(BINARY_CONTENT.length);
//            tx.commit();
//            tx = null;
            
            //
            // Write image renditions (original and translated)
            //
            tx = sess.beginTransaction();
            
            System.out.println("---- Creating image renditions: ----");
            DbImageRendition rend1 = child2.createImageRendition(LANG_ORIG, "thumbnail_small");
            DbImageRendition rend2 = child2.createImageRendition(LANG_ORIG, "thumbnail_large");
            DbImageRendition rend1_en = child2.createImageRendition("EN", "thumbnail_small");
            rend1.setContentType("image/png");
            rend1.setMaxWidth(160);
            rend1.setMaxHeight(150);
            rend1.setContent(BINARY_CONTENT);
            // rend1.setContentLength(BINARY_CONTENT.length);
            rend2.setContentType("image/gif");
            rend2.setMaxWidth(250);
            rend2.setMaxHeight(250);
            rend2.setContent(BINARY_CONTENT);
            // rend2.setContentLength(BINARY_CONTENT.length);
            rend1_en.setContentType("image/jpeg");
            rend1_en.setMaxWidth(180);
            rend1_en.setMaxHeight(180);
            rend1_en.setContent(BINARY_CONTENT_TRANSLATED);
            // rend1_en.setContentLength(BINARY_CONTENT_TRANSLATED.length);

            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;

            /*
             * root1
             *   +- child1(Aliases: CHILD1_ALIAS1, CHILD1_ALIAS2
             *             Locks: LOCK_NAME, CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE, EN: ...
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE, EN: ...)
             *       +- subchild1(Text: TXT_CONTENT, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1: Binary: Blob-Stream (length: 1 MB)
             *       +- subchild2(Aliases: SUBCHILD2_ALIAS1, SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3)
             *   +- child2(Aliases: CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE, EN: ...
             *             CHILD2_ATT_B_NAME: CHILD2_ATT_B_VALUE, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             *       +- subchild3(Aliases: SUBCHILD3_ALIAS)
             * 
             * root2
             * 
             */

            // Close session and open new session to avoid reading from cache.
            sess.close();
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();

            //
            // Read nodes
            //
            tx = sess.beginTransaction();
            
            System.out.println("---- Loading version object ----");
            ver = (DbVersion) sess.load(DbVersion.class, new Integer(ver_db_id));
            assertTrue("Check root node count", ver.allRootNodes().size() == 2);
            System.out.println("---- Getting root nodes ----");
            root1 = (DbNode) ver.allRootNodes().get(0);
            root2 = (DbNode) ver.allRootNodes().get(1);
            System.out.println("---- Getting child nodes count ----");
            assertTrue("Check 1. root node child count", root1.allChildNodes().size() == 2);
            assertTrue("Check 2. root node child count", root2.allChildNodes().size() == 0);
            System.out.println("---- Getting parent nodes of root nodes----");
            assertTrue(root1.getParentNode() == null);
            assertTrue(root2.getParentNode() == null);
            
            System.out.println("---- Getting child nodes of root node ----");
            child1 = (DbNode) root1.allChildNodes().get(0);
            child2 = (DbNode) root1.allChildNodes().get(1);
            long child1_db_id = child1.getNodeDbId();
            long child2_db_id = child2.getNodeDbId();
            assertTrue(child1.getNodeNumber() == child1_number);
            assertTrue(child2.getNodeNumber() == child2_number);
            System.out.println("---- Getting parent nodes of child nodes ----");
            assertSame(root1, child1.getParentNode());
            assertSame(root1, child2.getParentNode());
            System.out.println("---- Getting child nodes count ----");
            assertTrue("Check 1. child node child count", child1.allChildNodes().size() == 2);
            assertTrue("Check 2. child node child count", child2.allChildNodes().size() == 1);
            System.out.println("---- Getting child nodes of child nodes ----");
            subchild1 = (DbNode) child1.allChildNodes().get(0);
            subchild2 = (DbNode) child1.allChildNodes().get(1);
            subchild3 = (DbNode) child2.allChildNodes().get(0);
            System.out.println("---- Getting parent nodes of sub-child nodes ----");
            assertSame(child1, subchild1.getParentNode());
            assertSame(child1, subchild2.getParentNode());
            assertSame(child2, subchild3.getParentNode());
            System.out.println("---- Getting sub-child nodes count ----");
            assertTrue("Check 1. subnode child count", subchild1.allChildNodes().size() == 1);
            assertTrue("Check 2. subnode child count", subchild2.allChildNodes().size() == 0);
            assertTrue("Check 3. subnode child count", subchild3.allChildNodes().size() == 0);
            System.out.println("---- Getting child nodes of sub-child nodes ----");
            subsubchild1 = (DbNode) subchild1.allChildNodes().get(0);
            System.out.println("---- Getting parent nodes of sub-sub-child nodes ----");
            assertSame(subchild1, subsubchild1.getParentNode());
            System.out.println("---- Getting sub-sub-child nodes count ----");
            assertTrue("Check subsubnode child count", subsubchild1.allChildNodes().size() == 0);

            System.out.println("---- Getting database ids of nodes ----");
            long subchild1_db_id = subchild1.getNodeDbId();
            long subchild2_db_id = subchild2.getNodeDbId();
            long subchild3_db_id = subchild3.getNodeDbId();
            System.out.println("---- Getting node numbers of nodes ----");
            long subchild1_node_num = subchild1.getNodeNumber();
            long subchild2_node_num = subchild2.getNodeNumber();
            long subchild3_node_num = subchild3.getNodeNumber();
            assertTrue(subchild1_db_id > 0);  // database id was assigned
            assertTrue(subchild2_db_id > 0);  // database id was assigned
            assertTrue(subchild3_db_id > 0);  // database id was assigned
            assertTrue(subchild1_node_num > 0);  // node number was assigned
            assertTrue(subchild2_node_num > 0);  // node number was assigned
            assertTrue(subchild3_node_num > 0);  // node number was assigned

            // 
            // Read attributes
            //
            System.out.println("---- Reading attribute values of nodes ----");
            assertEquals(CHILD1_ATT_A_VALUE, child1.getAttribute(LANG_ORIG,   CHILD1_ATT_A_NAME)); 
            assertEquals(CHILD1_ATT_A_VALUE_TRANSLATED, child1.getAttribute("EN", CHILD1_ATT_A_NAME));
            assertEquals(CHILD1_ATT_B_VALUE, child1.getAttribute(LANG_ORIG,   CHILD1_ATT_B_NAME));
            assertEquals(CHILD1_ATT_B_VALUE_TRANSLATED, child1.getAttribute("EN", CHILD1_ATT_B_NAME));

            assertEquals(CHILD2_ATT_A_VALUE, child2.getAttribute(LANG_ORIG,   CHILD2_ATT_A_NAME)); 
            assertEquals(CHILD2_ATT_A_VALUE_TRANSLATED, child2.getAttribute("EN", CHILD2_ATT_A_NAME));
            assertEquals(CHILD2_ATT_B_VALUE, child2.getAttribute(LANG_ORIG,   CHILD2_ATT_B_NAME));
            assertEquals(CHILD2_ATT_B_VALUE_TRANSLATED, child2.getAttribute("EN", CHILD2_ATT_B_NAME));

            //
            // Read aliases
            //
            System.out.println("---- Getting alias count ----");
            assertTrue(child1.aliasCount() == 2);
            assertTrue(child2.aliasCount() == 2);
            assertTrue(subchild3.aliasCount() == 1);
            System.out.println("---- Reading alias names of nodes ----");
            assertEquals(CHILD1_ALIAS1, child1.getAlias(0));
            assertEquals(CHILD1_ALIAS2, child1.getAlias(1));
            assertEquals(CHILD2_ALIAS1, child2.getAlias(0));
            assertEquals(CHILD2_ALIAS2, child2.getAlias(1));
            assertEquals(SUBCHILD2_ALIAS1, subchild2.getAlias(0));
            assertEquals(SUBCHILD2_ALIAS2, subchild2.getAlias(1));
            assertEquals(SUBCHILD2_ALIAS3, subchild2.getAlias(2));
            assertEquals(SUBCHILD3_ALIAS, subchild3.getAlias(0));
            
            //
            // Read locks
            //
            System.out.println("---- Reading locks of nodes ----");
            assertTrue(root1.getLocks().isEmpty());
            assertTrue(root2.getLocks().isEmpty());
            assertTrue(child1.getLocks().size() == 2);
            
            DbNodeLock lock1 = child1.getLock(LOCK_NAME);
            assertTrue(lock1 != null);
            assertSame(child1, lock1.getLockedNode());
            assertEquals(USER_ID1, lock1.getUserId());
            assertEquals(currentTime, lock1.getCreationTime());
            assertEquals((long) 1000, lock1.getTimeout());
            
            DbNodeLock lock2 = child1.getLock(CHILD1_LOCK_NAME);
            assertTrue(lock2 != null);
            assertSame(child1, lock2.getLockedNode());
            assertEquals(USER_ID1, lock2.getUserId());
            assertEquals(currentTime, lock2.getCreationTime());
            assertEquals((long) 2000, lock2.getTimeout());
            
            assertTrue(child2.getLocks().size() == 2);
            DbNodeLock lock3 = child2.getLock(LOCK_NAME);
            DbNodeLock lock4 = child2.getLock(CHILD2_LOCK_NAME);
            assertTrue(lock3 != null);
            assertTrue(lock4 != null);
            
            System.out.println("---- Accessing text/binary node object ----");
            
            txt = subchild1.getTextContent(LANG_ORIG);
            assertEquals("txt/html", txt.getContentType());
            txt_en = subchild1.getTextContent("EN");
            assertEquals(TXT_CONTENT_TRANSLATED, txt_en.getContent());
            cont = child2.getBinaryContent(LANG_ORIG);
            assertEquals("image/jpeg", cont.getContentType());
            // assertTrue(cont.getContent().length() == BINARY_CONTENT.length);
            
            // assertTrue(tx.isActive());
            System.out.println("---- Commit ----");
            tx.commit();
            // assertTrue(! tx.isActive());
            tx = sess.beginTransaction();
            
            System.out.println("----");
            System.out.println("Reading clob/blob content within new transaction.");
            System.out.println("----");
            
            //
            // Read text content
            //
            System.out.println("---- Reading text content ----");
            txt = subchild1.getTextContent(LANG_ORIG);
            // sess.refresh(txt);
            assertEquals(TXT_CONTENT, txt.getContent());
            assertEquals(TXT_CONTENT.length(), txt.getContentLength());
            assertEquals("txt/html", txt.getContentType());
            txt_en = subchild1.getTextContent("EN");
            assertEquals(TXT_CONTENT_TRANSLATED, txt_en.getContent());
            assertEquals(TXT_CONTENT_TRANSLATED.length(), txt_en.getContentLength());
            
            //
            // Read binary content
            //
            System.out.println("---- Reading binary content ----");
            assertTrue(child1.allBinaryContent().isEmpty());
            assertTrue(child2.allBinaryContent().size() == 2);  // original and translated
            cont = child2.getBinaryContent(LANG_ORIG);
            cont.refreshContent(sess);  // needed because blob has been created in previous transaction  
            assertEquals("image/jpeg", cont.getContentType());
            assertTrue(cont.getContentLength() == BINARY_CONTENT.length);
            assertTrue(cont.getContent().length() == BINARY_CONTENT.length);
            cont_en = child2.getBinaryContent("EN");
            // assertEquals("image/jpeg", cont_en.getContentType());
            assertTrue(cont_en.getContentLength() == BINARY_CONTENT_TRANSLATED.length);
            assertTrue(cont_en.getContent().length() == BINARY_CONTENT_TRANSLATED.length);
            cont2 = subsubchild1.getBinaryContent(LANG_ORIG);
            assertTrue(testInputStreamMatches(cont2.getContent().getBinaryStream(), 1024*1024, (byte) 5));
            
            //
            // Read image renditions
            //
            System.out.println("---- Reading image renditions ----");
            assertTrue(child1.allImageRenditions().isEmpty());
            assertTrue(child2.allImageRenditions().size() == 3);  // original and translated
            rend1 = child2.getImageRendition(LANG_ORIG, "thumbnail_small");
            rend1_en = child2.getImageRendition("EN", "thumbnail_small");
            rend2 = child2.getImageRendition(LANG_ORIG, "thumbnail_large");
            assertEquals("image/png", rend1.getContentType());
            assertTrue(rend1.getMaxWidth() == 160);
            assertTrue(rend1.getMaxHeight() == 150);
            assertTrue(rend1.getContent().length == BINARY_CONTENT.length);
            assertTrue(rend1.getContentLength() == BINARY_CONTENT.length);
            assertEquals("image/gif", rend2.getContentType());
            assertTrue(rend2.getMaxWidth() == 250);
            assertTrue(rend2.getMaxHeight() == 250);
            assertTrue(rend2.getContent().length == BINARY_CONTENT.length);
            assertTrue(rend2.getContentLength() == BINARY_CONTENT.length);
            assertEquals("image/jpeg", rend1_en.getContentType());
            assertTrue(rend1_en.getMaxWidth() == 180);
            assertTrue(rend1_en.getMaxHeight() == 180);
            assertTrue(rend1_en.getContent().length == BINARY_CONTENT_TRANSLATED.length);
            assertTrue(rend1_en.getContentLength() == BINARY_CONTENT_TRANSLATED.length);
            
            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;
            
            System.out.println("----");
            System.out.println("Query nodes in new session.");
            System.out.println("----");
            sess.close();
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();
            
            tx = sess.beginTransaction();
            String condition = condition = "node.nodeType = '" + NODE_TYPE_IMAGE + "'";
            // List result = sess.createQuery(
            //     "select al.alias, al.node.nodeNumber, node from DbAlias al where al.version.versionDbId = " + 
            //     ver_db_id + " and al." + condition).list(); 
            Query a_query = sess.createQuery(
                 "select node from DbNode node " + 
                 "left join fetch node.aliasList " + 
                 // "left join fetch node.attributes " +
                 "left join fetch node.lastModifiedNode " +
                 // "left outer join DbNode realnode with node.lastModNodeDbId = realnode.nodeDbId " +
                 "where node.versionDbId = " + ver_db_id + " and " + condition);
            a_query.setMaxResults(1024*1024);
            List qlist = a_query.list();
//            sess.createCriteria(DbNode.class)
//                    .add(Restrictions.eq("versionDbId", new Integer(ver_db_id)))
//                    .add(Restrictions.eq("nodeType", NODE_TYPE_IMAGE))
//                    .setFetchMode("aliasList", FetchMode.JOIN)
//                    .setFetchMode("attributes", FetchMode.SELECT)
//                    .setFetchMode("lastModifiedNode", FetchMode.JOIN)
//                    .setFetchMode("lastModifiedNode.attributes", FetchMode.SELECT)
//                    .setMaxResults(1024*1024).list();
            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;
            // assertEquals(2, qlist.size());
            System.out.println("---- Query result: ----");
            for (Object row : qlist) {
                // Object[] cols = (Object[]) row;
                DbNode n = (DbNode) row; // cols[0];
                // DbNode rn = (DbNode) cols[1];
                System.out.println("Node-id/number: " + n.getNodeDbId() + "/" + n.getNodeNumber());
                // System.out.println("Real-node-number: " + ((rn == null) ? "" : (rn.getNodeDbId() + "/" + rn.getNodeNumber())));
                for (String a : n.getAliases()) {
                    System.out.println("Node-alias: " + a);
                }
                for (Object k : n.getAttributes().keySet()) {
                    System.out.println("Node-attribute: " + k);
                }
            }
            System.out.println("---------------------------");            
            
            System.out.println("----");
            System.out.println("Update of nodes in new session.");
            System.out.println("----");
            //
            // Test update of nodes in new session
            //
            sess.close();
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();
            
            tx = sess.beginTransaction();
            ver = (DbVersion) sess.load(DbVersion.class, new Integer(ver_db_id));

            System.out.println("---- Getting root nodes ----");
            root1 = (DbNode) ver.allRootNodes().get(0);
            root2 = (DbNode) ver.allRootNodes().get(1);
            
            System.out.println("---- Getting child nodes of 1st root node by ID ----");
            // DbNode ch1_node = getNodeById(sess, ver, child1_number);
            // DbNode ch2_node = getNodeById(sess, ver, child2_number);
            DbNode ch1_node = (DbNode) sess.load(DbNode.class, new Long(child1_db_id));
            DbNode ch2_node = (DbNode) sess.load(DbNode.class, new Long(child2_db_id));
            System.out.println("---- Getting position of child nodes ----");
            int ch1_pos = root1.allChildNodes().indexOf(ch1_node);
            int ch2_pos = root1.allChildNodes().indexOf(ch2_node);
            assertTrue(ch1_pos == 0);
            assertTrue(ch2_pos == 1);
            child1 = (DbNode) root1.allChildNodes().get(0);
            child2 = (DbNode) root1.allChildNodes().get(1);
            assertTrue(child1.getNodeNumber() == child1_number);
            assertTrue(child2.getNodeNumber() == child2_number);
            assertTrue(child1.equals(ch1_node));
            assertTrue(child2.equals(ch2_node));
            // subchild1 = (DbNode) child1.allChildNodes().get(0);
            subchild2 = (DbNode) child1.allChildNodes().get(1);

            System.out.println("---- Setting node type of 1st root node ----");
            root1.setNodeType("systemfolder");
            System.out.println("---- Setting node type of sub-child node ----");
            subchild2.setNodeType("section");

            System.out.println("---- Change, add, remove attributes ----");
            root1.setAttribute(LANG_ORIG, ROOT1_ATT_NAME, ROOT1_ATT_VALUE);  // add attribute
            child1.removeAttributes("EN", null);   // remove all EN translations
            child2.setAttribute(LANG_ORIG, CHILD2_ATT_A_NAME, CHILD2_ATT_A_VALUE_CHANGED); // change attribute
            child2.removeAttributes(null, CHILD2_ATT_B_NAME);
            
            System.out.println("---- Change, remove, add, move aliases ----");
            child1.replaceAlias(0, CHILD1_ALIAS1_CHANGED);   // change alias
            child1.deleteAlias(1);                           // remove alias CHILD1_ALIAS2
            child2.insertAlias(0, CHILD2_ALIAS_ADDED);       // add alias
            // move first alias to end of list:
            assertEquals(subchild2.aliasIndex(SUBCHILD2_ALIAS1), 0);
            assertTrue(subchild2.deleteAlias(SUBCHILD2_ALIAS1));
            subchild2.appendAlias(SUBCHILD2_ALIAS1);
            subchild2.appendAlias(CHILD1_ALIAS2);  // try moving alias from one node to another node within same session
            assertEquals(SUBCHILD2_ALIAS2, subchild2.getAlias(0));
            assertEquals(SUBCHILD2_ALIAS3, subchild2.getAlias(1));
            assertEquals(SUBCHILD2_ALIAS1, subchild2.getAlias(2));
            assertEquals(CHILD1_ALIAS2, subchild2.getAlias(3));

            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED
             *             Locks: LOCK_NAME, CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE)
             *       +- subchild1(Text: TXT_CONTENT, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             *       +- subchild3(Aliases: SUBCHILD3_ALIAS)
             * 
             * root2
             * 
             */

            System.out.println("---- Querying node by alias name ----");
            // Query q = sess.createQuery("from DbNode as node where :alias in elements(node.aliasList) and node.versionDbId = :verid");
            Query q = sess.createQuery("from DbAlias as al where al.alias = :aname and al.version.versionDbId = :verid");
            q.setString("aname", CHILD1_ALIAS1_CHANGED);
            q.setInteger("verid", ver_db_id);
            List result = q.list();
            assertTrue(result.size() == 1);
            DbAlias result_alias = (DbAlias) result.get(0);
            DbNode result_node = result_alias.getNode();
            assertEquals(result_node.getNodeDbId(), child1.getNodeDbId());
            
            System.out.println("---- Listing all alias names of node type 'section' ----");
            result = sess.createQuery(
                "select al.alias from DbAlias al where al.version.versionDbId = " + 
                ver_db_id + " and al.node.nodeType = 'section'").list();
            assertEquals(result.size(), 4);
            for (int i = 0; i < result.size(); i++) {
                System.out.println("Alias " + i + ": " + result.get(i));
            }

            System.out.println("---- Change, remove, add lock ----");
            DbNodeLock mylock = child1.getLock(CHILD1_LOCK_NAME);
            long updated_lock_time = System.currentTimeMillis();
            mylock.setCreationTime(updated_lock_time);
            child1.removeLock(LOCK_NAME);
            child2.addLock(LOCK_NAME_ADDED, USER_ID1, updated_lock_time, 1000);
            
            System.out.println("---- Change text content ----");
            DbNode mysubchild = (DbNode) child1.allChildNodes().get(0);
            txt = mysubchild.getTextContent(LANG_ORIG);
            txt.setContent(TXT_CONTENT_CHANGED); 
            
            System.out.println("---- Flushing ----");
            sess.flush();

            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE)
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             *       +- subchild3(Aliases: SUBCHILD3_ALIAS)
             * 
             * root2
             * 
             */
            
            System.out.println("---- Move 1. child at end of list ----");
            child1.removeChildNode(mysubchild);
            child1.addChildNode(mysubchild);
            
            // remove child node (subchild3)
            System.out.println("---- Remove child node (subchild3) ----");
            child2.removeChildNode(0);
            
            System.out.println("---- Trigger deletion of orphans ----");
            ver.deleteOrphans(sess);
            
            System.out.println("---- check cascading delete of alias name of removed child node ----");
            q = sess.createQuery("from DbAlias as al where al.alias = :aname and al.version.versionDbId = :verid");
            q.setString("aname", SUBCHILD3_ALIAS);
            q.setInteger("verid", ver_db_id);
            result = q.list();
            assertTrue(result.isEmpty());
            
            System.out.println("---- remove 2nd root node ----");
            ver.removeRootNode(root2);
            
            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;
            
            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE)
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             * 
             */
            
            // Close session and open new session to avoid reading from cache.
            sess.close();
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();

            //
            // Read updated nodes
            //
            System.out.println("---- Getting version object  ----");
            tx = sess.beginTransaction();
            ver = (DbVersion) sess.load(DbVersion.class, new Integer(ver_db_id));

            System.out.println("---- Check root node was removed  ----");
            assertTrue(ver.allRootNodes().size() == 1);
            
            System.out.println("---- Check updated root node ----");
            root1 = (DbNode) ver.allRootNodes().get(0);
            assertEquals("systemfolder", root1.getNodeType());
            
            System.out.println("---- Check count child nodes of root node ----");
            assertTrue(root1.allChildNodes().size() == 2);
            System.out.println("---- Getting child nodes of root node ----");
            child1 = (DbNode) root1.allChildNodes().get(0);
            child2 = (DbNode) root1.allChildNodes().get(1);
            
            System.out.println("---- Check updated attributes ----");
            assertTrue(root1.getAttributes().size() == 1);
            assertEquals(ROOT1_ATT_VALUE, root1.getAttribute(LANG_ORIG, ROOT1_ATT_NAME));
            assertEquals(CHILD1_ATT_A_VALUE, child1.getAttribute(LANG_ORIG, CHILD1_ATT_A_NAME));
            assertEquals(CHILD1_ATT_B_VALUE, child1.getAttribute(LANG_ORIG, CHILD1_ATT_B_NAME));
            assertTrue(child1.getAttribute("EN", CHILD1_ATT_A_NAME) == null);  // translation was removed
            assertTrue(child1.getAttribute("EN", CHILD1_ATT_B_NAME) == null);  // translation was removed
            assertEquals(CHILD2_ATT_A_VALUE_CHANGED, child2.getAttribute(LANG_ORIG, CHILD2_ATT_A_NAME));
            assertEquals(CHILD2_ATT_A_VALUE_TRANSLATED, child2.getAttribute("EN", CHILD2_ATT_A_NAME));
            assertTrue(child2.getAttribute(LANG_ORIG, CHILD2_ATT_B_NAME) == null);  // ATT_B was removed
            assertTrue(child2.getAttribute("EN", CHILD2_ATT_B_NAME) == null);  // ATT_B was removed
            
            System.out.println("---- Check updated alias names ----");
            assertTrue(child1.aliasCount() == 1);
            assertEquals(CHILD1_ALIAS1_CHANGED, child1.getAlias(0)); 
            assertTrue(child2.aliasCount() == 3);
            assertEquals(CHILD2_ALIAS_ADDED, child2.getAlias(0));
            assertEquals(CHILD2_ALIAS1, child2.getAlias(1));
            assertEquals(CHILD2_ALIAS2, child2.getAlias(2));

            System.out.println("---- Check updated locks ----");
            mylock = child1.getLock(CHILD1_LOCK_NAME);
            assertEquals(updated_lock_time, mylock.getCreationTime());
            assertTrue(child1.getLock(LOCK_NAME) == null);  // was removed
            assertTrue(child2.getLocks().size() == 3);
            DbNodeLock addedLock = child2.getLock(LOCK_NAME_ADDED);
            assertEquals(updated_lock_time, addedLock.getCreationTime());
            
            System.out.println("---- Check moved node ----");
            assertTrue(child1.allChildNodes().size() == 2);
            DbNode child_a = (DbNode) child1.allChildNodes().get(0);
            DbNode child_b = (DbNode) child1.allChildNodes().get(1);
            assertEquals(subchild2_db_id, child_a.getNodeDbId());
            assertEquals(subchild1_db_id, child_b.getNodeDbId());
            assertEquals(subchild2_node_num, child_a.getNodeNumber());
            assertEquals(subchild1_node_num, child_b.getNodeNumber());
            
            System.out.println("---- Check removed node ----");
            assertTrue(child2.allChildNodes().isEmpty());

            System.out.println("---- Check updated text content (original) ----");
            txt = child_b.getTextContent(LANG_ORIG);
            assertEquals(TXT_CONTENT_CHANGED, txt.getContent());
            assertEquals(TXT_CONTENT_CHANGED.length(), txt.getContentLength());
            System.out.println("---- Check updated text content (EN) ----");
            txt_en = child_b.getTextContent("EN");
            assertEquals(TXT_CONTENT_TRANSLATED, txt_en.getContent());
            assertEquals(TXT_CONTENT_TRANSLATED.length(), txt_en.getContentLength());

            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;

            System.out.println("---- Moving node from one parent to another parent ----");
            tx = sess.beginTransaction();
            child_a = (DbNode) child1.removeChildNode(0);
            long child_a_db_id = child_a.getNodeDbId();
            System.out.println("Old parent DB id: " + child1.getNodeDbId());
            System.out.println("New parent DB id: " + child2.getNodeDbId());
            System.out.println("Moved node DB id: " + child_a_db_id);
            child2.addChildNode(child_a);
            System.out.println("---- Commit ----");
            tx.commit();
            
            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE)
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             * 
             */
            
            tx = sess.beginTransaction();
            System.out.println("---- Check child count of new parent node ----");
            assertTrue(child2.allChildNodes().size() == 1);
            
            System.out.println("---- Check node id at new position ----");
            child_a = (DbNode) child2.allChildNodes().get(0);
            assertEquals(child_a.getNodeDbId(), child_a_db_id);
            System.out.println("---- Moving node back to previous parent ----");
            // child_a = (DbNode) child2.removeChildNode(0);
            child1.addChildNode(0, child_a);
            System.out.println("---- Check child count of parent nodes ----");
            assertTrue(child1.allChildNodes().size() == 2);
            assertTrue(child2.allChildNodes().size() == 0);
            System.out.println("---- Commit ----");
            tx.commit();
            tx = sess.beginTransaction();
            System.out.println("---- Check node id at new position ----");
            child_a = (DbNode) child1.allChildNodes().get(0);
            assertEquals(child_a.getNodeDbId(), child_a_db_id);
            System.out.println("---- Commit ----");
            tx.commit();
            tx = null;
            
            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: CHILD1_ATT_A_VALUE
             *             CHILD1_ATT_B_NAME: CHILD1_ATT_B_VALUE)
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             * 
             */
            

            //
            // Test concurrent update of nodes
            //
            System.out.println("----");
            System.out.println("Concurrent update test");
            System.out.println("----");
            final String ATT_UPDATED_SESSION1 = "Updated in session1";
            final String ATT_UPDATED_SESSION2 = "Updated in session2";
            final String ALIAS_ADDED_1 = "AliasSess1";
            final String ALIAS_ADDED_2 = "AliasSess2";
            Session sess1 = fact.openSession();
            Session sess2 = fact.openSession();
            long added1_node_number = -1;
            long added2_node_number = -1;
            try {
                System.out.println("---- Read node into session 1 ----");
                Transaction tx1 = sess1.beginTransaction();
                DbNode node1 = (DbNode) sess1.load(DbNode.class, new Long(child1.getNodeDbId()));
                String val_a = node1.getAttribute(LANG_ORIG, CHILD1_ATT_A_NAME);
                String val_b = node1.getAttribute(LANG_ORIG, CHILD1_ATT_B_NAME);
                System.out.println("Session1: Attribute " + CHILD1_ATT_A_NAME + ": " + val_a);
                System.out.println("Session1: Attribute " + CHILD1_ATT_B_NAME + ": " + val_b);
                for (Object ch : node1.allChildNodes()) {
                    DbNode child = (DbNode) ch;
                    System.out.println("Session1 Child: " + child.getNodeNumber());
                }
                System.out.println("---- Commit ----");
                tx1.commit();
                
                System.out.println("---- Read node into session 2 ----");
                Transaction tx2 = sess2.beginTransaction();
                DbNode node2 = (DbNode) sess2.load(DbNode.class, new Long(child1.getNodeDbId()));
                String val_x = node2.getAttribute(LANG_ORIG, CHILD1_ATT_A_NAME);
                String val_y = node2.getAttribute(LANG_ORIG, CHILD1_ATT_B_NAME);
                System.out.println("Session2: Attribute " + CHILD1_ATT_A_NAME + ": " + val_x);
                System.out.println("Session2: Attribute " + CHILD1_ATT_B_NAME + ": " + val_y);
                for (Object ch : node2.allChildNodes()) {
                    DbNode child = (DbNode) ch;
                    System.out.println("Session2 Child: " + child.getNodeNumber());
                }
                System.out.println("---- Commit ----");
                tx2.commit();
                
                System.out.println("---- Update node in session 1 ----");
                tx1 = sess1.beginTransaction();
                node1.appendAlias(ALIAS_ADDED_1);
                node1.setAttribute(LANG_ORIG, CHILD1_ATT_A_NAME, ATT_UPDATED_SESSION1);
                assertTrue(node1.allChildNodes().size() == 2);
                DbNode ch1 = (DbNode) node1.allChildNodes().get(0);
                DbNode ch2 = (DbNode) node1.allChildNodes().get(1);
                assertEquals(subchild2_node_num, ch1.getNodeNumber());
                assertEquals(subchild1_node_num, ch2.getNodeNumber());
                DbVersion ver1 = (DbVersion) sess1.load(DbVersion.class, new Integer(ver_db_id));
                DbNode added_child1 = ver1.createNode();
                node1.addChildNode(1, added_child1);
                System.out.println("---- Commit ----");
                tx1.commit();
                added1_node_number = added_child1.getNodeNumber();
                System.out.println("Added child in session 1: " + added1_node_number);
                
                System.out.println("---- Update node in session 2 ----");
                tx2 = sess2.beginTransaction();
                node2.appendAlias(ALIAS_ADDED_2);
                node2.setAttribute(LANG_ORIG, CHILD1_ATT_B_NAME, ATT_UPDATED_SESSION2);
                DbVersion ver2 = (DbVersion) sess2.load(DbVersion.class, new Integer(ver_db_id));
                DbNode added_child2 = ver2.createNode();
                node2.addChildNode(added_child2);

            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED, ALIAS_ADDED_1, ALIAS_ADDED_2
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: ATT_UPDATED_SESSION1
             *             CHILD1_ATT_B_NAME: ATT_UPDATED_SESSION2)
             *       +- subchild2(Type: section
             *                    Aliases: SUBCHILD2_ALIAS2, SUBCHILD2_ALIAS3, SUBCHILD2_ALIAS1, CHILD1_ALIAS2)
             *       +- added_child1
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *       +- added_child2
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             * 
             */

                // node2.addChildNode(1, added_child2);
                DbNode delNode = (DbNode) node2.allChildNodes().get(0);
                assertTrue(delNode.getNodeNumber() == subchild2_node_num);
                node2.removeChildNode(0);
                System.out.println("Removed child in session 2: " + delNode.getNodeNumber());
                System.out.println("---- Commit ----");
                tx2.commit();
                added2_node_number = added_child2.getNodeNumber();
                System.out.println("Added child in session 2: " + added2_node_number);
                
            /*
             * root1(Type: systemfolder
             *       ROOT1_ATT_NAME: ROOT1_ATT_VALUE)
             *   +- child1(Aliases: CHILD1_ALIAS1_CHANGED, ALIAS_ADDED_1, ALIAS_ADDED_2
             *             Locks: CHILD1_LOCK_NAME
             *             CHILD1_ATT_A_NAME: ATT_UPDATED_SESSION1
             *             CHILD1_ATT_B_NAME: ATT_UPDATED_SESSION2)
             *       +- added_child1
             *       +- subchild1(Text: TXT_CONTENT_CHANGED, EN: TXT_CONTENT_TRANSLATED)
             *            +- subsubchild1 
             *       +- added_child2
             *   +- child2(Aliases: CHILD2_ALIAS_ADDED, CHILD2_ALIAS1, CHILD2_ALIAS2
             *             Locks: LOCK_NAME, CHILD2_LOCK_NAME, LOCK_NAME_ADDED
             *             CHILD2_ATT_A_NAME: CHILD2_ATT_A_VALUE_CHANGED, EN: ...
             *             Binary: BINARY_CONTENT, BINARY_CONTENT_TRANSLATED)
             *             ImageRendition "thumbnail_small": ... EN: ...
             *             ImageRendition "thumbnail_large": ... 
             * 
             */
            } finally {
                System.out.println("---- Closing concurrent sessions ----");
                sess1.close();
                sess2.close();
            }
            //
            // Check concurrent update
            //
            System.out.println("----");
            System.out.println("---- Check concurrent update ----");
            System.out.println("----");
            sess.close();  // Close session and open new session to avoid reading from cache.
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();
            tx = sess.beginTransaction();
            DbNode node1 = (DbNode) sess.load(DbNode.class, new Long(child1.getNodeDbId()));
            String val_a = node1.getAttribute(LANG_ORIG, CHILD1_ATT_A_NAME);
            String val_b = node1.getAttribute(LANG_ORIG, CHILD1_ATT_B_NAME);
            System.out.println("After concurrent update:");
            for (Object al : node1.getAliases()) {
                System.out.println("Alias: " + al);
            }
            System.out.println("Attribute " + CHILD1_ATT_A_NAME + ": " + val_a);
            System.out.println("Attribute " + CHILD1_ATT_B_NAME + ": " + val_b);
            List list = node1.allChildNodes();
            
            assertEquals(val_a, ATT_UPDATED_SESSION1);
            assertEquals(val_b, ATT_UPDATED_SESSION2);
            assertTrue(list.size() == 3);  // 2 added nodes from 2 sessions, 1 removed node
            boolean added1 = false;
            boolean added2 = false;
            boolean removed1 = true;
            for (Object obj : list) {
                long node_num = ((DbNode) obj).getNodeNumber();
                System.out.println("Child: " + node_num);
                if (node_num == added1_node_number) added1 = true;
                if (node_num == added2_node_number) added2 = true;
                if (node_num == subchild2_node_num) removed1 = false;
            }
            assertTrue(added1);
            assertTrue(added2);
            assertTrue(removed1);
            
            tx.commit();
            tx = null;
            
            tx = sess.beginTransaction();
            child2 = (DbNode) sess.load(DbNode.class, new Long(child2.getNodeDbId()));
            child2.removeBinaryContent(LANG_ORIG);
            child2.removeBinaryContent("EN");
            
            tx.commit();
            tx = null;
            sess.close();  // Close session and open new session to avoid reading from cache.
            System.out.println("---- Opening new session ----");
            sess = fact.openSession();
            tx = sess.beginTransaction();

            child2 = (DbNode) sess.load(DbNode.class, new Long(child2.getNodeDbId()));
            assertTrue(child2.allBinaryContent().isEmpty());
            
            tx.commit();
            tx = null;
        } finally {
            sess.close();
        }
    }
    
    private void executeApplicationPropTests() throws Exception
    {
        //
        // Test reading and writing of application properties
        //
        
        // Create test data
        final int cnt = 10;
        String[] APP_PROP_NAME = new String[cnt];
        String[] APP_PROP_VALUE = new String[cnt];
        Map APP_PROP_MAP = new HashMap();
        for (int i = 0; i < cnt; i++) {
            APP_PROP_NAME[i] = "my.test.prop" + (i+1); 
            APP_PROP_VALUE[i] = "A " + (i+1) + ". property value.";
            APP_PROP_MAP.put(APP_PROP_NAME[i], APP_PROP_VALUE[i]);
        }

        Session sess = fact.openSession();
        Transaction tx = null;
        try {
            
            // Write application properties
            tx = sess.beginTransaction();
            for (int i = 0; i < cnt; i++) {
                DbApplicationProperty app_prop = new DbApplicationProperty();
                app_prop.setPropName(APP_PROP_NAME[i]);
                app_prop.setPropValue(APP_PROP_VALUE[i]);

                sess.save(app_prop);   // saveOrUpdate(app_prop);
            }
            tx.commit();
            tx = null;

            // Read application properties
            tx = sess.beginTransaction();
            List result = sess.createQuery("from DbApplicationProperty").list();
            tx.commit();
            tx = null;

            assertTrue("Result length okay", cnt == result.size());
            for (int i = 0; i < result.size(); i++) {
                DbApplicationProperty prop = (DbApplicationProperty) result.get(i);
                assertEquals(APP_PROP_MAP.get(prop.getPropName()), prop.getPropValue());
            }

        // } catch (Exception he) {
        //     if (tx != null) tx.rollback();
        //     throw he;
        } finally {
            sess.close();
        }
    }

    
    private void writeTextContent(String value, DbTextContent txt) throws Exception
    {
//        java.sql.Clob lob = txt.getContent();
//        Reader instream = new StringReader(value);
//        Writer out = lob.setCharacterStream(1);
//        long total_length = 0;
//        char[] buf = new char[1024];
//        int cnt;
//        while ((cnt = instream.read(buf)) >= 0) {
//            out.write(buf, 0, cnt);
//            total_length += cnt;
//        }
//        out.close();
//        txt.setContentLength(total_length);
        txt.setContent(value);
        // txt.setContentLength();
    }
    

    public DbNode getNodeById(Session dbSess, DbVersion ver, long node_num)
    {
        Query q = dbSess.createQuery("from DbNode where nodeNumber = :node_num and version.versionDbId = :ver_id");
        q.setLong("node_num", node_num);
        q.setInteger("ver_id", ver.getVersionDbId());
        return (DbNode) q.uniqueResult();
    }

    private DbVersion getVersionByName(DbStore store, String verId)
    {
        Iterator it = store.allVersions().iterator();
        while (it.hasNext()) {
            DbVersion v = (DbVersion) it.next();
            if (verId.equals(v.getVersionName())) {
                return v;
            }
        }
        return null;
    }


    public void listStores(Session sess)
    {
        Transaction tx = sess.beginTransaction();
        List result = sess.createQuery("from DbStore").list();
        tx.commit();
        tx = null;

        for (int i = 0; i < result.size(); i++) {
            DbStore store = (DbStore) result.get(i);
            out.println(store);

            out.println("  Versions in Store:");
            Iterator it = store.allVersions().iterator();
            while (it.hasNext()) {
                DbVersion v = (DbVersion) it.next();
                out.println("    " + v);
            }
        }
    }


    public void listPubExportAttributes(Session sess, int export_id)
    {
        Transaction tx = sess.beginTransaction();

        DbPublicationExport export =
            (DbPublicationExport) sess.load(DbPublicationExport.class, new Integer(export_id));
        out.println("Attributes of publication export with ID " + export_id + ":");
        Iterator it = export.getAttributes().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry att = (Map.Entry) it.next();
            out.println("    Name: " + att.getKey() + "  Value: " + att.getValue());
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
