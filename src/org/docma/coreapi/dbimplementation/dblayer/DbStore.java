/*
 * DbStore.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class DbStore
{
    private int storeDbId = 0;   // primary key; generated value; 0 means transient
    private String storeDisplayId;

    private Set versions = new LinkedHashSet();
    private Map properties = new HashMap();


    public DbStore()
    {
    }


    public int getStoreDbId()
    {
        return storeDbId;
    }

    void setStoreDbId(int storeDbId)
    {
        this.storeDbId = storeDbId;
    }


    public String getStoreDisplayId()
    {
        return storeDisplayId;
    }

    public void setStoreDisplayId(String storeDisplayId)
    {
        this.storeDisplayId = storeDisplayId;
    }


    Set getVersions()
    {
        return versions;
    }

    void setVersions(Set versions)
    {
        this.versions = versions;
    }

    public Set allVersions()
    {
        return Collections.unmodifiableSet(versions);
    }

    public void addVersion(DbVersion version)
    {
        version.setStore(this);
        getVersions().add(version);
    }

    public boolean removeVersion(DbVersion version)
    {
        boolean removed = getVersions().remove(version);
        if (removed) {
            version.setStore(null);
        }
        return removed;
    }


    public Map getProperties()
    {
        return properties;
    }

    public void setProperties(Map properties)
    {
        this.properties = properties;
    }


    public String toString()
    {
        return "Store DB-ID: " + getStoreDbId() +
                "  Store Display-ID: " + getStoreDisplayId();
    }


    public boolean equals(Object obj)
    {
        if ((obj == null) || !(obj instanceof DbStore)) {
            return false;
        }
        final DbStore other = (DbStore) obj;
        if ((this.storeDisplayId == null) ? (other.storeDisplayId != null) : !this.storeDisplayId.equals(other.storeDisplayId)) {
            return false;
        }
        return true;
    }


    public int hashCode()
    {
        return (this.storeDisplayId != null) ? this.storeDisplayId.hashCode() : 0;
    }


}
