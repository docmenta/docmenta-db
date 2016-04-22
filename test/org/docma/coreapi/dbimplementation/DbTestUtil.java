/*
 * DbTestUtil.java
 */
package org.docma.coreapi.dbimplementation;

import org.docma.hibernate.CreateDDL;
import org.docma.hibernate.HibernateUtil;
import org.hibernate.*;

/**
 *
 * @author MP
 */
public class DbTestUtil 
{
    public static SessionFactory setUp_Derby(String db_path)
    {
        final String derby_system_path = "c:\\work\\derby_test_dbs\\derby_system";
        
        final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
        final String CONNECTION_URL = "jdbc:derby:directory:" + db_path + ";create=true";
        final String SQL_DIALECT = "org.hibernate.dialect.DerbyDialect";
        final String DB_USER = null;
        final String DB_PASSWD = null;

        System.setProperty("derby.system.home", derby_system_path);

        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        return HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    public static SessionFactory setUp_MySQL(String CONNECTION_URL, String DB_USER, String DB_PASSWD)
    {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String SQL_DIALECT = "org.hibernate.dialect.MySQLDialect";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        return HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }

    public static SessionFactory setUp_SQLServer(String CONNECTION_URL, String DB_USER, String DB_PASSWD)
    {
        final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        final String SQL_DIALECT = "org.hibernate.dialect.SQLServer2008Dialect";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        return HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    public static SessionFactory setUp_Oracle(String CONNECTION_URL, String DB_USER, String DB_PASSWD)
    {
        final String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
        final String SQL_DIALECT = "org.hibernate.dialect.Oracle10gDialect";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        return HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    
    public static SessionFactory setUp_Postgres(String CONNECTION_URL, String DB_USER, String DB_PASSWD)
    {
        final String JDBC_DRIVER = "org.postgresql.Driver";
        final String SQL_DIALECT = "org.hibernate.dialect.PostgreSQLDialect";
                
        CreateDDL.initDB(JDBC_DRIVER, CONNECTION_URL, SQL_DIALECT, DB_USER, DB_PASSWD, null);
        
        return HibernateUtil.getSessionFactory(JDBC_DRIVER, CONNECTION_URL, 
                                               SQL_DIALECT, DB_USER, DB_PASSWD);
    }
    

}
