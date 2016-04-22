/*
 * ApplicationPropertiesDbImpl.java
 */

package org.docma.app.dbimplementation;

import java.util.*;
import org.hibernate.Session;
import org.hibernate.Transaction;

import org.docma.coreapi.ApplicationProperties;
import org.docma.coreapi.DocException;
import org.docma.coreapi.DocConstants;
import org.docma.coreapi.dbimplementation.dblayer.*;

/**
 *
 * @author MP
 */
public class ApplicationPropertiesDbImpl implements ApplicationProperties
{
    private org.hibernate.SessionFactory factory;
    // private org.hibernate.Session sess;
    private Map properties = new HashMap();


    public void ApplicationPropertiesDbImpl(org.hibernate.SessionFactory factory)
    throws Exception
    {
        this.factory = factory;
        // this.sess = factory.openSession();
        
        loadProperties();
    }


    public String getProperty(String name)
    {
        DbApplicationProperty prop = (DbApplicationProperty) properties.get(name);
        if (prop == null) return null;
        return prop.getPropValue();
    }

    public void setProperty(String name, String value) throws DocException
    {
        Session sess = factory.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            setProperty(sess, name, value);
            tx.commit();
        } catch (Exception ex) {
            if (tx != null) tx.rollback();
            throw new DocException(ex);
        } finally {
            sess.close();
        }
    }

    public void setProperties(String[] names, String[] values) throws DocException
    {
        Session sess = factory.openSession();
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            for (int i=0; i < names.length; i++) {
                setProperty(sess, names[i], values[i]);
            }
            tx.commit();
        } catch (Exception ex) {
            if (tx != null) tx.rollback();
            throw new DocException(ex);
        } finally {
            sess.close();
        }
    }

    private void setProperty(Session sess, String name, String value) throws Exception
    {
        DbApplicationProperty prop = (DbApplicationProperty) properties.get(name);
        if (prop == null) {
            prop = new DbApplicationProperty();
            prop.setPropName(name);
            prop.setPropValue(value);
            sess.save(prop);
            properties.put(name, prop);
        } else {
            prop.setPropValue(value);
            sess.update(prop);
        }
    }

    private void loadProperties() throws Exception
    {
        if (DocConstants.DEBUG) {
            System.out.println("Loading application properties:");
        }
        Session sess = factory.openSession();
        List result;
        Transaction tx = null;
        try {
            tx = sess.beginTransaction();
            result = sess.createQuery("from DbApplicationProperty").list();
            tx.commit();
        } catch (Exception ex) {
            if (tx != null) tx.rollback();
            throw ex;
        } finally {
            sess.close();
        }

        properties.clear();
        for (int i = 0; i < result.size(); i++) {
            DbApplicationProperty prop = (DbApplicationProperty) result.get(i);
            if (DocConstants.DEBUG) {
                System.out.println("  Property Name: " + prop.getPropName());
                System.out.println("  Property Value: " + prop.getPropValue());
            }
            properties.put(prop.getPropName(), prop);
        }
    }

//    private void saveProperties() throws Exception
//    {
//        Iterator it = properties.values().iterator();
//        while (it.hasNext()) {
//            DbApplicationProperty prop = (DbApplicationProperty) it.next();
//            sess.update(prop);
//        }
//    }

}
