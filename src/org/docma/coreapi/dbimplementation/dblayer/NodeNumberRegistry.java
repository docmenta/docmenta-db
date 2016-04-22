/*
 * NodeNumberRegistry.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

import java.util.*;

/**
 *
 * @author MP
 */
public class NodeNumberRegistry
{
    private static Map nextNumberMap = new HashMap();

    static synchronized long getNewNumber(DbVersion version)
    {
        long current_next_num = getNextNodeNumber(version);

        // increase next node number
        setNextNodeNumber(version, current_next_num + 1);

        return current_next_num;
    }

    static synchronized void registerNumber(DbVersion version, long num)
    {
        long current_next_num = getNextNodeNumber(version);
        if (num >= current_next_num) {
            setNextNodeNumber(version, num + 1);
        }
    }
    
    public static synchronized long nextNodeNumberOfVersion(DbVersion version) 
    {
        return getNextNodeNumber(version);
    }

    private static long getNextNodeNumber(DbVersion version)
    {
        Integer versionId = new Integer(version.getVersionDbId());
        Long next_long = (Long) nextNumberMap.get(versionId);
        long new_num = version.getNextNodeNumber();  // version is a cached object,
                                                     // i.e. new_num may not be up to date.
        if (next_long != null) {
            long long_id = next_long.longValue();
            if (long_id > new_num) {  // new_num is not up to date
                new_num = long_id;
            }
        }
        return new_num;
    }

    private static void setNextNodeNumber(DbVersion version, long next_num)
    {
        version.setNextNodeNumber(next_num);
        Integer versionId = new Integer(version.getVersionDbId());
        nextNumberMap.put(versionId, new Long(next_num));
    }
}
