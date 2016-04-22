/*
 * DbUserGroup.java
 */

package org.docma.userapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class DbUserGroup
{
    private int groupDbId;
    private String groupName;
    private Map properties = new HashMap();
    private Set users = new HashSet();

    public DbUserGroup()
    {
    }

    public int getGroupDbId()
    {
        return groupDbId;
    }

    public void setGroupDbId(int groupDbId)
    {
        this.groupDbId = groupDbId;
    }

    public String getGroupName()
    {
        return groupName;
    }

    public void setGroupName(String groupName)
    {
        this.groupName = groupName;
    }


    Set getUsers()
    {
        return users;
    }

    void setUsers(Set users)
    {
        this.users = users;
    }

    public Set allUsers()
    {
        return Collections.unmodifiableSet(users);
    }

    public boolean addUser(DbUser user)
    {
        boolean added = getUsers().add(user);
        user.getGroups().add(this);
        return added;
    }

    public boolean removeUser(DbUser user)
    {
        if (getUsers().remove(user)) {
            user.getGroups().remove(this);
            return true;
        }
        return false;
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
        if (! (obj instanceof DbUserGroup)) {
            return false;
        }
        final DbUserGroup other = (DbUserGroup) obj;
        if ((this.groupName == null) ? (other.groupName != null) : !this.groupName.equals(other.groupName)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        return (this.groupName != null) ? this.groupName.hashCode() : 0;
    }


}
