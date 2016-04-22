/*
 * NodeInfoDbImpl.java
 * 
 *  Copyright (C) 2014  Manfred Paula, http://www.docmenta.org
 *   
 *  This file is part of Docmenta. Docmenta is free software: you can 
 *  redistribute it and/or modify it under the terms of the GNU Lesser 
 *  General Public License as published by the Free Software Foundation, 
 *  either version 3 of the License, or (at your option) any later version.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Docmenta.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.docma.coreapi.dbimplementation;

import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.DbNode;
import org.docma.coreapi.implementation.DocAttributes;
import org.hibernate.Session;

/**
 *
 * @author MP
 */
public class NodeInfoDbImpl implements NodeInfo
{
    private String node_id;
    private String alias;
    private String title = null;
    
    public NodeInfoDbImpl(DbNode node, DocNodeContext ctx) 
    {
        node_id = DbUtil.formatNodeNumber(node.getNodeNumber());
        alias = (node.aliasCount() > 0) ? node.getAlias(0) : null;
        
        // Get title attribute
        long lastmodId = node.getLastModNodeDbId();
        DbNode realnode = node;
        if ((lastmodId > 0) && (lastmodId != node.getNodeDbId())) {
            Session db_sess = ctx.getDbSession();
            realnode = (DbNode) db_sess.get(DbNode.class, lastmodId);  // load(DbNode.class, lastmodId);
        }
        if (realnode == null) {
            title = "";
        } else {
            String lang_id = ctx.getTranslationMode();
            String lang_key = (lang_id == null) ? DbConstants.LANG_ORIG : lang_id;  // transform null to string for database layer

            title = realnode.getAttribute(lang_key, DocAttributes.TITLE);
            if ((title == null) && (lang_id != null)) {  // if no translated value exists in translation mode
                // return value of original language
                title = realnode.getAttribute(DbConstants.LANG_ORIG, DocAttributes.TITLE);
            }
            if (title == null) {
                title = "";
            }
        }
    }

    public String getId() 
    {
        return node_id;
    }

    public String getTitle() 
    {
        return title;
    }

    public String getAlias() 
    {
        return alias;
    }

    @Override
    public int hashCode() 
    {
        String id = getId();
        return (id == null) ? super.hashCode() : id.hashCode();
    }

    @Override
    public boolean equals(Object obj) 
    {
        if (obj == null) {
            return false;
        }
        if (! (obj instanceof NodeInfo)) {
            return false;
        }
        final String other_id = ((NodeInfo) obj).getId();
        return (other_id != null) && other_id.equals(getId());
    }
    
}
