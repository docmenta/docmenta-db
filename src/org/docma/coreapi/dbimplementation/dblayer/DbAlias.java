/*
 * DbAlias.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbAlias implements java.io.Serializable
{
    // private long aliasDbId = 0;  // generated value; 0 means transient
    private DbVersion version;
    private DbNode node;
    private String alias;
    // private int pos;

    DbAlias()
    {
    }

//    public long getAliasDbId() 
//    {
//        return aliasDbId;
//    }
//
//    public void setAliasDbId(long aliasDbId) 
//    {
//        this.aliasDbId = aliasDbId;
//    }

    public DbVersion getVersion() 
    {
        return version;
    }

    void setVersion(DbVersion version) 
    {
        this.version = version;
    }

    public DbNode getNode() 
    {
        return node;
    }

    void setNode(DbNode node) 
    {
        this.node = node;
    }

    public String getAlias() 
    {
        return alias;
    }

    public void setAlias(String alias) 
    {
        this.alias = alias;
    }

//    int getPos() 
//    {
//        return pos;
//    }
//
//    void setPos(int pos) 
//    {
//        this.pos = pos;
//    }


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
        final DbAlias other = (DbAlias) obj;
        if ((this.alias == null) ? (other.alias != null) : !this.alias.equals(other.alias)) {
            return false;
        }
        if (this.version == null) {
            return (other.version == null);
        }
        if (other.version == null) {
            return false;
        }
        if (this.version.persisted() || other.version.persisted()) {
            if (this.version.getVersionDbId() != other.version.getVersionDbId()) {
                return false;
            }
        } else {
            if (this.version != other.version) {
                return false;
            }
        }
        return true;
    }


}
