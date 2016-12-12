/*
 * DocGroupDbImpl.java
 */

package org.docma.coreapi.dbimplementation;

import java.util.*;
import org.docma.coreapi.*;
import org.docma.coreapi.dbimplementation.dblayer.*;
import org.docma.util.Log;


/**
 *
 * @author MP
 */
public class DocGroupDbImpl extends DocNodeDbImpl implements DocGroup
{
    private ArrayList nodeList = null;  // helper list -> see getChildNodes

    DocGroupDbImpl(DocNodeContext docContext, DbNode node)
    {
        super(docContext, node);
    }

    private void refreshChildNodes()
    {
        nodeList = null;
    }

    /* --------------  Methods used in sub-classes  ---------------- */

    void fireChildAddedEvent(DocNode newChild)
    {
        getDocContext().nodeAddedEvent(this, newChild);
    }

    void fireChildRemovedEvent(DocNode child)
    {
        getDocContext().nodeRemovedEvent(this, child);
    }

    void fireChildMovedEvent(DocNode child)
    {
        getDocContext().nodeRemovedEvent(this, child);
        getDocContext().nodeAddedEvent(this, child);
    }

    /* --------------  Interface DocGroup ---------------------- */

    public void refresh()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            super.refresh();
            nodeList = null;   // refreshChildNodes();
        }
    }

    public DocNode[] getChildNodes()
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            if (nodeList == null) {  // else nodeList.clear();
                if (DocConstants.DEBUG) {
                    Log.info("Getting child nodes from DB backend node.");
                }
                boolean started = startLocalTransaction();
                try {
                    List children = getDbNode().allChildNodes();
                    nodeList = new ArrayList(children.size() + 10);
                    for (int i = 0; i < children.size(); i++) {
                        DbNode db_child = (DbNode) children.get(i);
                        DocNodeDbImpl childNode = ctx.createDocNodeFromDbNode(db_child);
                        if (childNode != null) {
                            childNode.setParentGroup(this);
                            nodeList.add(childNode);
                        } else {
                            Log.warning("createDocNodeFromDbNode() returned null. Skipping node in getChildNodes().");
                        }
                    }
                    commitLocalTransaction(started);
                } catch (Exception ex) {
                    nodeList = null;  // refreshChildNodes(); reload children on next call
                    rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
                }
            }
            DocNode[] nodeArr = new DocNode[nodeList.size()];
            nodeList.toArray(nodeArr);
            return nodeArr;
        }
    }

    public int getChildPos(DocNode childNode)
    {
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction();
            try {
                if (nodeList == null) getChildNodes();
                int pos = nodeList.indexOf(childNode);
                commitLocalTransaction(started);
                return pos;
            } catch (Exception ex) {
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
                return -1;  // is never reached
            }
        }
    }

    public DocNode appendChild(DocNode newChild)
    {
        return insertBefore(newChild, null);
    }

    public DocNode insertBefore(DocNode newChild, DocNode refChild) 
    {
        DocGroupDbImpl oldParent = null;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction(true);
            try {
                DocNodeDbImpl newChildImpl = (DocNodeDbImpl) newChild;
                oldParent = (DocGroupDbImpl) newChildImpl.getParentGroup();
                // int oldPos = -1;
                // if (oldParent != null) {
                //     oldPos = oldParent.getChildPos(newChild);
                // }

                // Get affected database objects
                DbNode thisNodeDb = getDbNode();
                DbNode newChildDb = newChildImpl.getDbNode();
                DbNode oldParentDb = (oldParent != null) ? oldParent.getDbNode() : null;

                // Allow update of affected database objects
                prepareUpdate(thisNodeDb);  // allow update
                prepareUpdate(thisNodeDb.allChildNodes());  // position of child nodes may change
                prepareUpdate(newChildDb);  // allow update
                if ((oldParentDb != null) && (oldParentDb != thisNodeDb)) {
                    prepareUpdate(oldParentDb);  // allow update
                    prepareUpdate(oldParentDb.allChildNodes());  // position of child nodes may change
                }

                // Update database objects
                if (oldParent == this) {  // child node changes position -> remove
                    boolean removed = thisNodeDb.removeChildNode(newChildDb);
                    if (! removed) Log.warning("Could not remove child node in method DocGroupDbImpl.insertBefore().");
                }
                if (refChild == null) {
                    thisNodeDb.addChildNode(newChildDb);
                } else {
                    DbNode refChildDb = ((DocNodeDbImpl) refChild).getDbNode();
                    int ins_pos = thisNodeDb.allChildNodes().indexOf(refChildDb);
                    if (ins_pos < 0) {
                        thisNodeDb.addChildNode(newChildDb);
                    } else {
                        thisNodeDb.addChildNode(ins_pos, newChildDb);
                    }
                }
                commitLocalTransaction(started);
                newChildImpl.setParentGroup(this);
                nodeList = null;   // refreshChildNodes();  reload on next call
            } catch (Exception ex) {
                nodeList = null;  // refreshChildNodes();  reload on next call
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        if (oldParent == this) {  // child node changed position
            // int newPos = getChildPos(newChild);
            fireChildMovedEvent(newChild);
        } else {
            if (oldParent != null) {
                oldParent.refreshChildNodes();
                oldParent.fireChildRemovedEvent(newChild);
            }
            fireChildAddedEvent(newChild);
        }
        return newChild;
    }

    public DocNode removeChild(DocNode child)
    {
        boolean removed = false;
        DocNodeContext ctx = getDocContext();
        synchronized (ctx) {
            boolean started = startLocalTransaction(true);
            try {
                // int delpos = getChildPos(child);
                DocNodeDbImpl childImpl = (DocNodeDbImpl) child;
                DbNode thisNodeDb = getDbNode();
                DbNode childDb = childImpl.getDbNode();
                prepareUpdate(thisNodeDb);  // allow update
                prepareUpdate(thisNodeDb.allChildNodes());  // position of child nodes may change
                prepareUpdate(childDb);     // allow update
                removed = thisNodeDb.removeChildNode(childDb);
                commitLocalTransaction(started);

                childImpl.setParentGroup(null);
                nodeList = null;   // refreshChildNodes();  reload on next call
                if (DocConstants.DEBUG && !removed) {
                    Log.warning("Could not remove child node in DocGroupDbImpl.removeChild().");
                }
            } catch (Exception ex) {
                nodeList = null;  // refreshChildNodes();  reload on next call
                rollbackLocalTransactionRuntime(started, ex);  // throws runtime exception
            }
        }
        if (! removed) {
            throw new DocRuntimeException("Removing child failed: node not in child list.");
        }
        fireChildRemovedEvent(child);
        return child;
    }


}
