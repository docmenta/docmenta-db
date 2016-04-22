/*
 * CreateDDL.java
 */

package org.docma.hibernate;

import java.io.File;
import org.hibernate.cfg.*;
import org.hibernate.tool.hbm2ddl.*;

/**
 *
 * @author MP
 */
public class CreateDDL
{

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        initDB("com.mysql.jdbc.Driver", 
               "jdbc:mysql://localhost:3306/docmentadb",
               "org.hibernate.dialect.MySQLDialect",
               "docma_usr",
               "docma_pw", 
               new File("C:\\TEMP\\hibernate_export\\docma_schema.sql"));
    }
    
    public static void initDB(DbConnectionData conndata) 
    {
        initDB(conndata, null);
    }
    
    public static void initDB(DbConnectionData conndata, File outfile) 
    {
        initDB(conndata.getDriverClassName(), 
               conndata.getConnectionURL(),
               conndata.getDbDialect(),
               conndata.getUserId(),
               conndata.getUserPwd(), 
               outfile);
    }

    public static void initDB(String jdbcDriver, 
                              String connURL, 
                              String dialect, 
                              String usr, 
                              String pw,
                              File outFile) 
    {
        Configuration cfg = HibernateUtil.getConfiguration(jdbcDriver,
                                                           connURL,
                                                           dialect,
                                                           usr,
                                                           pw);

        SchemaExport exp = new SchemaExport(cfg);
        exp.setFormat(true);
        if (outFile != null) {
            exp.setOutputFile(outFile.getAbsolutePath());
            System.out.println("\nWriting Hibernate Schema to file: " + outFile.getAbsolutePath());
        }
        System.out.println("\nWriting Hibernate Schema to database:" + connURL);

        exp.create(true, true);

        System.out.println("\nHibernate Schema Export finished.");
    }


    public static void createDB(DbConnectionData conndata, File outfile) 
    {
        createDB(conndata.getDriverClassName(), 
                 conndata.getConnectionURL(),
                 conndata.getDbDialect(),
                 conndata.getUserId(),
                 conndata.getUserPwd(), 
                 outfile);
    }
            
    public static void createDB(String jdbcDriver, 
                                String connURL, 
                                String dialect, 
                                String usr, 
                                String pw,
                                File outFile) 
    {
        Configuration cfg = HibernateUtil.getConfiguration(jdbcDriver,
                                                           connURL,
                                                           dialect,
                                                           usr,
                                                           pw);

        SchemaExport exp = new SchemaExport(cfg);
        exp.setFormat(true);
        if (outFile != null) {
            exp.setOutputFile(outFile.getAbsolutePath());
            System.out.println("\nWriting Hibernate Schema to file: " + outFile.getAbsolutePath());
        }
        System.out.println("\nWriting Hibernate Schema to database:" + connURL);

        final boolean JUST_DROP = false;
        final boolean JUST_CREATE = true;
        exp.execute(true, true, JUST_DROP, JUST_CREATE);

        System.out.println("\nHibernate Schema Export finished.");
    }

}
