/*
 * DbUtil.java
 */

package org.docma.coreapi.dbimplementation;

import java.util.*;
import java.io.*;
import java.sql.DriverManager;
import org.docma.coreapi.*;
import org.docma.coreapi.implementation.VersionIdFactory;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.util.Log;
import org.docma.hibernate.*;
import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DbUtil
{
    public static void initDatabase(DbConnectionData conndata)
    {
        initDatabase(conndata, null);
    }
    
    public static void initDatabase(DbConnectionData conndata, File ddl_log_dir)
    {
        Log.info("Initializing database: " + conndata.getConnectionURL());
        File ddl_log_file = null;
        if (ddl_log_dir != null) {
            if (! ddl_log_dir.exists()) {
                ddl_log_dir.mkdirs();
            }
            ddl_log_file = new File(ddl_log_dir, "init_db.sql");
        }
        CreateDDL.createDB(conndata, ddl_log_file);
        writeSchemaVersion(conndata, DbConstants.SCHEMA_VERSION_1);
        Log.info("Initialization of database finished.");
    }
    
    public static void releaseDatabase(DbConnectionData conndata)
    {
        // Close any open database connections:
        HibernateUtil.closeSessionFactory(conndata);
        
        // If the connection references an embedded database, then also
        // shut down the database instance:
        if (DbConstants.DB_EMBEDDED_DRIVER.equals(conndata.getDriverClassName())) {
            String con_url = conndata.getConnectionURL();
            con_url += con_url.trim().endsWith(";") ? "shutdown=true" : ";shutdown=true";
            if (DocConstants.DEBUG) {
                Log.info("Shutting down embedded database: " + con_url);
            }
            String msg = "Shutdown of embedded database finished.";
            try {
                DriverManager.getConnection(con_url);
            } catch (Exception ex) {
                msg = "Embedded database shutdown result: " + ex.getMessage();
            }
            Log.info(msg);
        }
    }
    
    public static boolean checkTablesExist(DbConnectionData conndata)
    {
        boolean res = false;
        SessionFactory dbfact = HibernateUtil.getSessionFactory(conndata);
        Session sess = dbfact.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            List result = sess.createQuery("from DbSchemaInfo").list();
            res = (result != null) && (result.size() >= 0);
            tx.commit();
        } catch (Exception e) {
            Log.info("Failed to execute query \"from DbSchemaInfo\": " + e.getMessage());
            if (tx != null) tx.rollback();
        } finally {
            sess.close();
        }
        Log.info("Check tables exist: " + res + " (" + conndata.getConnectionURL() + ")");
        return res;
    }
    
    /**
     * Transform database node number into node-id of interface DocNode.
     *
     * @param nodeNumber
     * @return
     */
    static String formatNodeNumber(long nodeNumber)
    {
        return "" + nodeNumber;
    }

    /**
     * Transform node-id of interface DocNode into database node number.
     *
     * @param nodeNumber
     * @return
     */
    static long parseNodeNumber(String nodeNumber)
    {
        return Long.parseLong(nodeNumber);
    }

    public static DbStore getDbStore(Session dbSession, String storeId)
    {
        if (DocConstants.DEBUG) {
            Log.info("Getting DbStore for display id: " + storeId);
        }
        Query q = dbSession.createQuery("from DbStore where storeDisplayId = :store_id");
        q.setString("store_id", storeId);
        return (DbStore) q.uniqueResult();
    }

    public static DbVersion getDbVersion(DbStore db_store, DocVersionId verId, VersionIdFactory versionIdFactory)
    {
        Set v_set = db_store.allVersions();
        Iterator it = v_set.iterator();
        while (it.hasNext()) {
            DbVersion db_ver = (DbVersion) it.next();
            String vname = db_ver.getVersionName();
            try {
                if (versionIdFactory.createVersionId(vname).equals(verId)) {
                    return db_ver;
                }
            } catch (Exception ex) {
                Log.error("Invalid version name: " + vname);
            }
        }
        return null;
    }

    public static DbNode getDbNode(Session dbSession, int versionDbId, String nodeNum)
    {
        long node_number = parseNodeNumber(nodeNum);
        Query q = dbSession.createQuery("from DbNode where versionDbId = :v_id and nodeNumber = :node_num");
        q.setInteger("v_id", versionDbId);
        q.setLong("node_num", node_number);
        return (DbNode) q.uniqueResult();
    }

    private static void writeSchemaVersion(DbConnectionData conn_data, String schema_version)
    {
        SessionFactory dbfact = HibernateUtil.getSessionFactory(conn_data);
        Session sess = dbfact.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            DbSchemaInfo db_info = (DbSchemaInfo) sess.get(DbSchemaInfo.class, DbConstants.INFO_SCHEMA_VERSION);
            if (db_info == null) {
                db_info = new DbSchemaInfo();
                db_info.setInfoName(DbConstants.INFO_SCHEMA_VERSION);
                db_info.setInfoValue(schema_version);
                sess.save(db_info);
                Log.info("Setting schema version: " + schema_version);
            } else {
                String old_version = db_info.getInfoValue();
                if (! schema_version.equals(old_version)) {
                    db_info.setInfoValue(schema_version);
                    Log.info("Schema version updated from " + old_version + " to " + schema_version);
                }
            }
            tx.commit();
        } catch (Exception e) {
            Log.info("Failed to set schema version '" + schema_version + "': " + e.getMessage());
            if (tx != null) tx.rollback();
        } finally {
            sess.close();
        }
    }
    
}
