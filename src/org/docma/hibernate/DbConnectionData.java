/*
 * DbConnectionData.java
 *
 *  Copyright (C) 2014  Manfred Paula, http://www.docmenta.org
 *   
 *  This file is part of Docmenta. Docmenta is free software: you can 
 *  redistribute it and/or modify it under the terms of the GNU Lesser 
 *  General Public License as published by the Free Software Foundation, 
 *  either version 3 of the License, or (at your option) any later version.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Docmenta.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package org.docma.hibernate;

/**
 *
 * @author MP
 */
public class DbConnectionData implements Cloneable
{
    String driverClassName;
    String connectionURL;
    String dbdialect;
    String dbuser;
    String dbpasswd;

    public DbConnectionData() 
    {
    }
    
    public DbConnectionData(String driver, String url, String dialect, String usr, String pw)
    {
        this.driverClassName = driver;
        this.connectionURL = url;
        this.dbdialect = dialect;
        this.dbuser = usr;
        this.dbpasswd = pw;
    }
    
    public String getDriverClassName() 
    {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) 
    {
        this.driverClassName = driverClassName;
    }

    public String getConnectionURL() 
    {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) 
    {
        this.connectionURL = connectionURL;
    }

    public String getDbDialect() 
    {
        return dbdialect;
    }

    public void setDbDialect(String dbdialect) 
    {
        this.dbdialect = dbdialect;
    }

    public String getUserId() 
    {
        return dbuser;
    }

    public void setUserId(String dbuser) 
    {
        this.dbuser = dbuser;
    }

    public String getUserPwd() 
    {
        return dbpasswd;
    }

    public void setUserPwd(String dbpasswd) 
    {
        this.dbpasswd = dbpasswd;
    }

    public String toString()
    {
        if (dbuser != null) {
            return driverClassName + " " + connectionURL + " " + dbuser + " " + dbpasswd;
        } else {
            return driverClassName + " " + connectionURL;
        }
    }

    public boolean equals(Object obj)
    {
        if (! (obj instanceof DbConnectionData)) {
            return false;
        }
        DbConnectionData other = (DbConnectionData) obj;
        return driverClassName.equals(other.driverClassName) &&
               connectionURL.equals(other.connectionURL) &&
               ((dbdialect == null) ? (other.dbdialect == null) : dbdialect.equals(other.dbdialect)) &&
               ((dbuser == null) ? (other.dbuser == null) : dbuser.equals(other.dbuser)) &&
               ((dbpasswd == null) ? (other.dbpasswd == null) : dbpasswd.equals(other.dbpasswd));
    }

    public int hashCode()
    {
        return toString().hashCode();
    }

    public Object clone()
    {
        try {
            return super.clone();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
