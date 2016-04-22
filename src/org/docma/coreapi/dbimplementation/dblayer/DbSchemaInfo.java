/*
 * DbSchemaInfo.java
 */
package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbSchemaInfo 
{
    private String infoName;   // primary key
    private String infoValue;

    public DbSchemaInfo()
    {
    }

    public String getInfoName() 
    {
        return infoName;
    }

    public void setInfoName(String infoName) 
    {
        this.infoName = infoName;
    }

    public String getInfoValue() 
    {
        return infoValue;
    }

    public void setInfoValue(String infoValue) 
    {
        this.infoValue = infoValue;
    }

    @Override
    public String toString() 
    {
        return "DbSchemaInfo{" + "infoName=" + infoName + ", infoValue=" + infoValue + '}';
    }
    

}
