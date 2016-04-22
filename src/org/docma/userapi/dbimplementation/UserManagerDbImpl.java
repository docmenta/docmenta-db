/*
 * UserManagerDbImpl.java
 */

package org.docma.userapi.dbimplementation;

import java.util.*;

import org.hibernate.*;

import org.docma.coreapi.*;
import org.docma.userapi.*;
import org.docma.userapi.dbimplementation.dblayer.*;

/**
 *
 * @author MP
 */
public class UserManagerDbImpl implements UserManager
{
    private final long SESS_TIMEOUT = 1000*60*60*8;  // reopen hibernate session after 8 hours
    private final int MODE_READ = 0;
    private final int MODE_UPDATE = 1;

    private org.hibernate.SessionFactory factory = null;
    private org.hibernate.Session sess = null;
    private org.hibernate.Transaction tx = null;

    private int currentWorkMode = -1;
    private long sessOpenedTime;

    /* --------------  Constructor  --------------- */

    public UserManagerDbImpl(SessionFactory factory)
    {
        this.factory = factory;
        this.sess = null;
    }

    protected void finalize() throws Throwable
    {
        if (sess != null) {
            try { sess.close(); } catch (Throwable th) {}
        }
        super.finalize();
    }

    /* --------------  Private helper methods  --------------- */

    private void openWork(int mode)
    {
        long now = System.currentTimeMillis();
        boolean reopen = (mode != currentWorkMode) || 
                         ((now - sessOpenedTime) > SESS_TIMEOUT);
        if (reopen && (sess != null)) {
            try { sess.close(); } catch (Throwable th) {}
            sess = null;
        }
        if (reopen || (sess == null) || !sess.isOpen()) {
            sess = factory.openSession();
            sess.setDefaultReadOnly(mode == MODE_READ);
            currentWorkMode = mode;
            sessOpenedTime = System.currentTimeMillis();
        }
        tx = sess.beginTransaction();
    }

    private void commitWork()
    {
        tx.commit();
        tx = null;
    }

    private void rollbackWorkSilent(Exception ex)
    {
        if (tx != null) {
            tx.rollback();
            tx = null;
        }
    }

    private void rollbackWork(Exception ex) throws DocException
    {
        rollbackWorkSilent(ex);
        if (ex instanceof DocException) throw (DocException) ex;
        else throw new DocException(ex);
    }

    private void rollbackWorkRuntime(Exception ex)
    {
        rollbackWorkSilent(ex);
        if (ex instanceof DocRuntimeException) throw (DocRuntimeException) ex;
        else throw new DocRuntimeException(ex);
    }

    private void closeWork()
    {
        // If MODE_UPDATE, then close session (i.e. do not keep objects in cache).
        // If MODE_READ do nothing here, i.e. session is kept open until timeout occurs!
        // This improves performance, if many successive read operations are
        // executed (objects are kept in cache, i.e. do not have to be read from
        // database on every call of a getXxxx()).
        if (currentWorkMode == MODE_UPDATE) {
            if (sess != null) sess.close();
            sess = null;
        }
    }

    /* --------------  Interface UserManager  --------------- */

    public synchronized String createUser(String userName) throws DocException
    {
        try {
            openWork(MODE_UPDATE);

            Query q = sess.createQuery("from DbUser where userName = :usr_name");
            q.setString("usr_name", userName);
            List result = q.list();
            if (result.size() > 0) {
                throw new DocException("A user with name '" + userName + "' already exists.");
            }

            DbUser usr = new DbUser();
            usr.setUserName(userName);
            sess.save(usr);

            commitWork();
            return "" + usr.getUserDbId();
        } catch (Exception ex) {
            rollbackWork(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void deleteUser(String userId) throws DocException
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_UPDATE);

            Query q = sess.createQuery("delete from DbUser where userDbId = :usr_id");
            q.setInteger("usr_id", id_num);
            int count = q.executeUpdate();

            commitWork();
            if (count < 1) {
                throw new DocException("Could not delete user with id " + userId);
            }
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized String[] getUserIds()
    {
        try {
            openWork(MODE_READ);

            List result = sess.createQuery("from DbUser").list();
            String[] ids = new String[result.size()];
            for (int i=0; i < ids.length; i++) {
                DbUser usr = (DbUser) result.get(i);
                ids[i] = "" + usr.getUserDbId();
            }
            commitWork();
            return ids;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized String getUserIdFromName(String userName)
    {
        try {
            openWork(MODE_READ);

            Query q = sess.createQuery("from DbUser where userName = :usr_name");
            q.setString("usr_name", userName);
            List result = q.list();
            if (result.isEmpty()) {
                return null;
            }
            if (result.size() > 1) {
                throw new DocRuntimeException("Username is not unique: " + userName);
            }
            DbUser usr = (DbUser) result.get(0);
            int user_id = usr.getUserDbId();
            commitWork();

            return "" + user_id;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized String getUserNameFromId(String userId)
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_READ);

            DbUser usr = (DbUser) sess.get(DbUser.class, new Integer(id_num));
            String usr_name = (usr == null) ? null : usr.getUserName();

            commitWork();
            return usr_name;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void setUserName(String userId, String newUserName) throws DocException
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_UPDATE);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(id_num));
            usr.setUserName(newUserName);

            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }

    public synchronized void setPassword(String userId, String newPassword) throws DocException
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_UPDATE);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(id_num));
            usr.setPasswd(newPassword);

            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized boolean verifyUserNamePassword(String userName, String password)
    {
        try {
            openWork(MODE_READ);

            Query q = sess.createQuery("from DbUser where userName = :usr_name");
            q.setString("usr_name", userName);
            List result = q.list();
            if (result.isEmpty()) {
                return false;
            }
            if (result.size() > 1) {
                throw new DocRuntimeException("Username is not unique: " + userName);
            }
            DbUser usr = (DbUser) result.get(0);
            String realpw = usr.getPasswd();

            commitWork();
            return password.equals(realpw);
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return false;
        } finally {
            closeWork();
        }
    }


    public synchronized String getUserProperty(String userId, String propName)
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_READ);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(id_num));
            String val = (String) usr.getProperties().get(propName);

            commitWork();
            return val;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void setUserProperty(String userId, String propName, String propValue) throws DocException
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_UPDATE);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(id_num));
            if (propValue == null) {
                usr.getProperties().remove(propName);
            } else {
                usr.getProperties().put(propName, propValue);
            }
            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized void setUserProperties(String userId, String[] propNames, String[] propValues) throws DocException
    {
        try {
            int id_num = Integer.parseInt(userId);
            openWork(MODE_UPDATE);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(id_num));
            Map props = usr.getProperties();
            for (int i=0; i < propNames.length; i++) {
                String propName = propNames[i];
                String propValue = propValues[i];
                if (propValue == null) {
                    props.remove(propName);
                } else {
                    props.put(propName, propValue);
                }
            }
            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized String createGroup(String groupName) throws DocException
    {
        try {
            openWork(MODE_UPDATE);

            Query q = sess.createQuery("from DbUserGroup where groupName = :grp_name");
            q.setString("grp_name", groupName);
            List result = q.list();
            if (result.size() > 0) {
                throw new DocException("A group with name '" + groupName + "' already exists.");
            }

            DbUserGroup grp = new DbUserGroup();
            grp.setGroupName(groupName);
            sess.save(grp);

            commitWork();
            return "" + grp.getGroupDbId();
        } catch (Exception ex) {
            rollbackWork(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void deleteGroup(String groupId) throws DocException
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            Query q = sess.createQuery("delete from DbUserGroup where groupDbId = :grp_id");
            q.setInteger("grp_id", id_num);
            int count = q.executeUpdate();

            commitWork();
            if (count < 1) {
                throw new DocException("Could not delete usergroup with id " + groupId);
            }
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized String[] getGroupIds()
    {
        try {
            openWork(MODE_READ);

            List result = sess.createQuery("from DbUserGroup").list();
            String[] ids = new String[result.size()];
            for (int i=0; i < ids.length; i++) {
                DbUserGroup grp = (DbUserGroup) result.get(i);
                ids[i] = "" + grp.getGroupDbId();
            }
            commitWork();
            return ids;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized String getGroupNameFromId(String groupId)
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_READ);

            DbUserGroup grp = (DbUserGroup) sess.get(DbUserGroup.class, new Integer(id_num));
            String grp_name = (grp == null) ? null : grp.getGroupName();

            commitWork();
            return grp_name;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized String getGroupIdFromName(String groupName)
    {
        try {
            openWork(MODE_READ);

            Query q = sess.createQuery("from DbUserGroup where groupName = :grp_name");
            q.setString("grp_name", groupName);
            List result = q.list();
            if (result.isEmpty()) {
                return null;
            }
            if (result.size() > 1) {
                throw new DocRuntimeException("Groupname is not unique: " + groupName);
            }
            DbUserGroup grp = (DbUserGroup) result.get(0);
            int group_id = grp.getGroupDbId();
            commitWork();

            return "" + group_id;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void setGroupName(String groupId, String newGroupName) throws DocException
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(id_num));
            grp.setGroupName(newGroupName);

            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized String getGroupProperty(String groupId, String propName)
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_READ);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(id_num));
            String val = (String) grp.getProperties().get(propName);

            commitWork();
            return val;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized void setGroupProperty(String groupId, String propName, String propValue) throws DocException
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(id_num));
            if (propValue == null) {
                grp.getProperties().remove(propName);
            } else {
                grp.getProperties().put(propName, propValue);
            }
            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized void setGroupProperties(String groupId, String[] propNames, String[] propValues) throws DocException
    {
        try {
            int id_num = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(id_num));
            Map props = grp.getProperties();
            for (int i=0; i < propNames.length; i++) {
                String propName = propNames[i];
                String propValue = propValues[i];
                if (propValue == null) {
                    props.remove(propName);
                } else {
                    props.put(propName, propValue);
                }
            }
            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized boolean isUserInGroup(String userId, String groupId)
    {
        String[] gids = getGroupsOfUser(userId);
        List glist = Arrays.asList(gids);
        return glist.contains(groupId);
//        try {
//            int grp_idnum = Integer.parseInt(groupId);
//            int usr_idnum = Integer.parseInt(userId);
//            openWork(MODE_READ);
//
//            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(grp_idnum));
//            DbUser usr = (DbUser) sess.get(DbUser.class, new Integer(usr_idnum));
//            if (usr == null) return false;
//            boolean is_in_grp = grp.allUsers().contains(usr);
//
//            commitWork();
//            return is_in_grp;
//        } catch (Exception ex) {
//            rollbackWorkRuntime(ex);
//            return false;
//        } finally {
//            closeWork();
//        }
    }


    public synchronized String[] getUsersInGroup(String groupId)
    {
        try {
            int grp_idnum = Integer.parseInt(groupId);
            openWork(MODE_READ);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(grp_idnum));
            Set usr_set = grp.allUsers();
            String[] ids = new String[usr_set.size()];
            Iterator it = usr_set.iterator();
            for (int i=0; i < ids.length; i++) {
                DbUser usr = (DbUser) it.next();
                ids[i] = "" + usr.getUserDbId();
            }
            commitWork();
            return ids;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public synchronized String[] getGroupsOfUser(String userId)
    {
        try {
            int usr_idnum = Integer.parseInt(userId);
            openWork(MODE_READ);

            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(usr_idnum));
            Set grp_set = usr.allGroups();
            String[] ids = new String[grp_set.size()];
            Iterator it = grp_set.iterator();
            for (int i=0; i < ids.length; i++) {
                DbUserGroup grp = (DbUserGroup) it.next();
                ids[i] = "" + grp.getGroupDbId();
            }
            commitWork();
            return ids;
        } catch (Exception ex) {
            rollbackWorkRuntime(ex);
            return null;
        } finally {
            closeWork();
        }
    }


    public void setGroupsOfUser(String userId, String[] groupIds) throws DocException 
    {
        try {
            int usr_idnum = Integer.parseInt(userId);
            ArrayList<Integer> add_grp_list = new ArrayList<Integer>(groupIds.length);
            for (int i=0; i < groupIds.length; i++) {
                add_grp_list.add(Integer.parseInt(groupIds[i]));
            }
            openWork(MODE_UPDATE);
            
            DbUser usr = (DbUser) sess.load(DbUser.class, new Integer(usr_idnum));
            Iterator it = usr.allGroups().iterator();
            while (it.hasNext()) {
                DbUserGroup grp = (DbUserGroup) it.next();
                // Integer grp_id = new Integer(grp.getGroupDbId());
                
                // Remove all currently assigned groups that are not in the new list of groups.
                // If the currently assigned group is in the new list, then the
                // group is not removed and therefore does not have to be added
                // (i.e. the group needs to be removed from the add_grp_list).
                
                // if (! add_grp_list.remove(grp_id)) { 
                usr.removeGroup(grp);
                // }
            }
            for (Integer grp_id : add_grp_list) {
                DbUserGroup grp = (DbUserGroup) sess.get(DbUserGroup.class, grp_id);
                if (grp == null) continue;
                usr.addGroup(grp);
            }
            commitWork();
        } catch (Exception ex) {
            rollbackWork(ex);
        } finally {
            closeWork();
        }
    }


    public synchronized boolean addUserToGroup(String userId, String groupId) throws DocException
    {
        int cnt = addUsersToGroup(new String[] {userId}, groupId);
        return (cnt > 0);
    }


    public synchronized int addUsersToGroup(String[] userIds, String groupId) throws DocException
    {
        try {
            int grp_idnum = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(grp_idnum));
            int cnt_added = 0;
            for (int i=0; i < userIds.length; i++) {
                int usr_idnum = Integer.parseInt(userIds[i]);
                DbUser usr = (DbUser) sess.get(DbUser.class, new Integer(usr_idnum));
                if (usr == null) continue;
                if (grp.addUser(usr)) ++cnt_added;
            }
            commitWork();
            return cnt_added;
        } catch (Exception ex) {
            rollbackWork(ex);
            return 0;
        } finally {
            closeWork();
        }
    }


    public synchronized boolean removeUserFromGroup(String userId, String groupId) throws DocException
    {
        int cnt = removeUsersFromGroup(new String[] {userId}, groupId);
        return (cnt > 0);
    }


    public synchronized int removeUsersFromGroup(String[] userIds, String groupId) throws DocException
    {
        try {
            int grp_idnum = Integer.parseInt(groupId);
            openWork(MODE_UPDATE);

            DbUserGroup grp = (DbUserGroup) sess.load(DbUserGroup.class, new Integer(grp_idnum));
            int cnt_removed = 0;
            for (int i=0; i < userIds.length; i++) {
                int usr_idnum = Integer.parseInt(userIds[i]);
                DbUser usr = (DbUser) sess.get(DbUser.class, new Integer(usr_idnum));
                if (usr == null) continue;
                if (grp.removeUser(usr)) ++cnt_removed;
            }
            commitWork();
            return cnt_removed;
        } catch (Exception ex) {
            rollbackWork(ex);
            return 0;
        } finally {
            closeWork();
        }
    }

}
