/*
 * DbReference.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 * Note: This class is not used and can be removed.
 *       References are implemented as normal DbNode objects
 *       (see org.docma.coreapi.dbimplementation.DocReferenceDbImpl.java).
 * 
 * @author MP
 */
public class DbReference extends DbNode
{
    private String targetAlias;


    DbReference()
    {
    }

    public String getTargetAlias()
    {
        return targetAlias;
    }

    public void setTargetAlias(String targetAlias)
    {
        this.targetAlias = targetAlias;
    }


}
