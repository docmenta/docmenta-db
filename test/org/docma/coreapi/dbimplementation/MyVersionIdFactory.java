/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.implementation.*;
import java.lang.reflect.*;

import org.docma.coreapi.*;

/**
 *
 * @author MP
 */
public class MyVersionIdFactory implements VersionIdFactory
{
    Class verIdClass = null;

    public MyVersionIdFactory() 
    {
    }

    public MyVersionIdFactory(Class verIdClass)
    {
        this.verIdClass = verIdClass;
    }

    public DocVersionId createVersionId(String ver_id) throws DocException
    {
        if (verIdClass == null) {
            return new MyVersionId(ver_id);
        } else {
            try {
                Constructor con = verIdClass.getConstructor(new Class[] { String.class });
                Object obj = con.newInstance(new Object[] { ver_id });
                return (DocVersionId) obj;
            } catch (InvocationTargetException ite) {
                throw (DocException) ite.getTargetException();
            } catch (Exception ex) {
                throw new DocRuntimeException(ex);
            }
        }
    }
    
}
