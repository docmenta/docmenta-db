/*
 * HibernateUtil.java
 */

package org.docma.hibernate;

import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import org.docma.coreapi.DocConstants;
import org.docma.coreapi.dbimplementation.DbConstants;

import org.hibernate.cfg.*;
import org.hibernate.SessionFactory;

import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.util.Log;
// import org.docma.userapi.dbimplementation.dblayer.*;

/**
 * Hibernate Utility class with a convenient method to get Session Factory object.
 *
 * @author MP
 */
public class HibernateUtil
{
    private static Properties extraProps = null;
    private static Properties extraEmbeddedProps = null;
    private static Map<DbConnectionData, SessionFactory> sessionFactories = null;

//    static {
//        try {
//            // Create the SessionFactory from standard (hibernate.cfg.xml)
//            // config file.
//
//            SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();
//        } catch (Throwable ex) {
//            // Log the exception.
//            System.err.println("Initial SessionFactory creation failed." + ex);
//            throw new ExceptionInInitializerError(ex);
//        }
//    }


    public static Configuration getConfiguration(DbConnectionData conndata)
    {
        return getConfiguration(conndata.getDriverClassName(),
                                conndata.getConnectionURL(),
                                conndata.getDbDialect(),
                                conndata.getUserId(),
                                conndata.getUserPwd());
    }

    public static Configuration getConfiguration(String driverClassName,
                                                 String connectionURL,
                                                 String dbdialect,
                                                 String dbuser,
                                                 String dbpasswd)
    {
        Configuration cfg = new Configuration();
        if (connectionURL.trim().toLowerCase().startsWith("java:")) {
            cfg.setProperty("hibernate.connection.datasource", connectionURL);
        } else {
            cfg.setProperty("hibernate.connection.url", connectionURL);
        }
        cfg.setProperty("hibernate.dialect", dbdialect);
        cfg.setProperty("hibernate.connection.driver_class", driverClassName);
        if (dbuser != null) {
            cfg.setProperty("hibernate.connection.username", dbuser);
            cfg.setProperty("hibernate.connection.password", dbpasswd);
        }

        cfg.setProperty("hibernate.id.new_generator_mappings", "true");
        // cfg.setProperty("hibernate.max_fetch_depth", "0");
        cfg.setProperty("hibernate.show_sql", DocConstants.DEBUG ? "true" : "false");
        if (DbConstants.DB_EMBEDDED_DRIVER.equals(driverClassName)) {
            cfg.addProperties(getEmbeddedConnectionProperties());
        } else {   // external database
            cfg.addProperties(getExternalConnectionProperties());
        }
        cfg.setNamingStrategy(ImprovedNamingStrategy.INSTANCE);

        cfg.addClass(DbSchemaInfo.class);
        cfg.addClass(DbApplicationProperty.class);
        // cfg.addClass(DbPubExportAttribute.class);
        cfg.addClass(DbPublicationExport.class);
        cfg.addClass(DbPubExportLob.class);
        cfg.addClass(DbStore.class);
        cfg.addClass(DbVersion.class);
        cfg.addClass(DbNode.class);
        cfg.addClass(DbAlias.class);
        cfg.addClass(DbNodeLock.class);
        // cfg.addClass(DbReference.class);
        cfg.addClass(DbBinaryContent.class);
        cfg.addClass(DbBinaryLob.class);
        cfg.addClass(DbTextContent.class);
        cfg.addClass(DbImageRendition.class);
        cfg.addClass(DbContentRevision.class);
        cfg.addClass(DbRevisionLob.class);

        return cfg;
    }


    public static SessionFactory getSessionFactory(String driverClassName,
                                                   String connectionURL,
                                                   String dbdialect,
                                                   String dbuser,
                                                   String dbpasswd) 
    {
        DbConnectionData conndata = new DbConnectionData();
        conndata.setDriverClassName(driverClassName);
        conndata.setConnectionURL(connectionURL);
        conndata.setDbDialect(dbdialect);
        conndata.setUserId(dbuser);
        conndata.setUserPwd(dbpasswd);
        return getSessionFactory(conndata);
    }
    
    public static synchronized SessionFactory getSessionFactory(DbConnectionData conndata)
    {
        if (sessionFactories == null) {
            sessionFactories = new HashMap<DbConnectionData, SessionFactory>(200);
        }
        SessionFactory factory = sessionFactories.get(conndata);
        if (factory == null) {
            Configuration cfg = getConfiguration(conndata);
            factory = cfg.buildSessionFactory();
            sessionFactories.put((DbConnectionData) conndata.clone(), factory);
        }
        return factory;
    }
    
    public static synchronized void closeSessionFactory(DbConnectionData conndata)
    {
        if (sessionFactories == null) {
            return;
        }
        SessionFactory factory = sessionFactories.get(conndata);
        if (factory != null) {
            sessionFactories.remove(conndata);
            factory.close();
        }
    }
    
    private static void addDefaultConnectionProperties(Properties props)
    {
        if (! (props.containsKey("hibernate.connection.useUnicode") || 
               props.containsKey("hibernate.connection.CharSet") || 
               props.containsKey("hibernate.connection.characterEncoding"))) {
            props.setProperty("hibernate.connection.useUnicode", "true");
            props.setProperty("hibernate.connection.CharSet", "utf8");
            props.setProperty("hibernate.connection.characterEncoding", "utf8");
        }
    }
    
    private static synchronized Properties getExternalConnectionProperties()
    {
        if (extraProps == null) {
            extraProps = new Properties();
        }
        addDefaultConnectionProperties(extraProps);
        return extraProps;
    }
    
    public static synchronized void setExternalConnectionProperties(File propsFile)
    {
        extraProps = loadProperties(propsFile);
    }
    
    private static synchronized Properties getEmbeddedConnectionProperties()
    {
        if (extraEmbeddedProps == null) {
            extraEmbeddedProps = new Properties();
        }
        addDefaultConnectionProperties(extraEmbeddedProps);
        return extraEmbeddedProps;
    }
    
    public static synchronized void setEmbeddedConnectionProperties(File propsFile)
    {
        extraEmbeddedProps = loadProperties(propsFile);
    }
    
    private static synchronized Properties loadProperties(File propsFile)
    {
        if (propsFile == null) {
            return null;
        }
        Log.info("Loading DB connection properties: " + propsFile);
        FileInputStream fin = null;
        Properties props = null;
        try {
            props = new Properties();
            fin = new FileInputStream(propsFile);
            props.load(fin);
        } catch (Exception ex) {
            ex.printStackTrace();
            Log.error("Could not load DB connection properties: " + propsFile);
            props = null;
        } finally {
            if (fin != null) {
                try { fin.close(); } catch (Exception ex) { ex.printStackTrace(); }
            }
        }
        return props;
    }

}
