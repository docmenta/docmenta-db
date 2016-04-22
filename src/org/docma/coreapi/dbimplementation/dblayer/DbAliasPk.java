/*
 * DbAliasPk.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbAliasPk implements java.io.Serializable
{
    private int versionDbId;
    private String alias;

    DbAliasPk()
    {
    }

    public int getVersionDbId() 
    {
        return versionDbId;
    }

    void setVersionDbId(int versionDbId) 
    {
        this.versionDbId = versionDbId;
    }

    public String getAlias() 
    {
        return alias;
    }

    public void setAlias(String alias) 
    {
        this.alias = alias;
    }

    @Override
    public int hashCode() 
    {
        return (this.alias != null) ? this.alias.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DbAliasPk other = (DbAliasPk) obj;
        if (this.versionDbId != other.versionDbId) {
            return false;
        }
        if ((this.alias == null) ? (other.alias != null) : !this.alias.equals(other.alias)) {
            return false;
        }
        return true;
    }


}
