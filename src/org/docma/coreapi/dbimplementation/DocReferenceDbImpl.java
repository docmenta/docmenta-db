/*
 * DocReferenceDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.coreapi.implementation.DocAttributes;

/**
 *
 * @author MP
 */
public class DocReferenceDbImpl extends DocNodeDbImpl implements DocReference
{
    private static final String ATT_TARGET_REFERENCE = DocAttributes.SYS_PREFIX + "reference.target_alias";


    DocReferenceDbImpl(DocNodeContext docContext, DbNode dbNode)
    {
        super(docContext, dbNode);
    }

    /* --------------  Interface DocReference ---------------------- */

    public String getTargetAlias()
    {
        return getAttribute(ATT_TARGET_REFERENCE, null);
    }

    public void setTargetAlias(String alias)
    {
        setAttribute(ATT_TARGET_REFERENCE, alias, null);
    }


}
