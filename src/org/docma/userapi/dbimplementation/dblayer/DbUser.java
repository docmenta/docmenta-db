/*
 * DbUser.java
 */

package org.docma.userapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class DbUser
{
    private int userDbId;      // primary key, generated
    private String userName;   // unique
    private String passwd;
    private Map properties = new HashMap();
    private Set groups = new HashSet();

    public DbUser()
    {
    }


    Set getGroups()
    {
        return groups;
    }

    void setGroups(Set groups)
    {
        this.groups = groups;
    }

    public Set allGroups()
    {
        return Collections.unmodifiableSet(groups);
    }

    public boolean addGroup(DbUserGroup group)
    {
        boolean added = getGroups().add(group);
        group.getUsers().add(this);
        return added;
    }

    public boolean removeGroup(DbUserGroup group)
    {
        if (getGroups().remove(group)) {
            group.getUsers().remove(this);
            return true;
        }
        return false;
    }


    public String getPasswd()
    {
        return passwd;
    }

    public void setPasswd(String passwd)
    {
        this.passwd = passwd;
    }

    public int getUserDbId()
    {
        return userDbId;
    }

    public void setUserDbId(int userDbId)
    {
        this.userDbId = userDbId;
    }

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public Map getProperties()
    {
        return properties;
    }

    public void setProperties(Map properties)
    {
        this.properties = properties;
    }


    public boolean equals(Object obj)
    {
        if (! (obj instanceof DbUser)) {
            return false;
        }
        final DbUser other = (DbUser) obj;
        if ((this.userName == null) ? (other.userName != null) : !this.userName.equals(other.userName)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        return (this.userName != null) ? this.userName.hashCode() : 0;
    }

}
