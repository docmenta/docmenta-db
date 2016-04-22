/*
 * DbConstants.java
 */

package org.docma.coreapi.dbimplementation;

/**
 *
 * @author MP
 */
public class DbConstants
{
    // SchemaInfo names
    public static final String INFO_SCHEMA_VERSION = "SchemaVersion";
    
    // SchemaInfo values
    public static final String SCHEMA_VERSION_1 = "1";

    // Embedded database properties
    public static final String DB_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DB_EMBEDDED_DIALECT = "org.hibernate.dialect.DerbyDialect";

    // Node types for database implementation of store
    final static String NODE_TYPE_GROUP = "group";
    final static String NODE_TYPE_IMAGE = "image";
    final static String NODE_TYPE_FILE = "file";
    final static String NODE_TYPE_XML = "xml";
    final static String NODE_TYPE_REFERENCE = "reference";

    // Language code representing original language in database
    // Note: Empty string cannot be used to represent original language,
    //       as Oracle converts empty string to NULL (great feature;).
    public final static String LANG_ORIG = "-";

}
