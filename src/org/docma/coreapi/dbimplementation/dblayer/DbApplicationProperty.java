/*
 * DbApplicationProperty.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class DbApplicationProperty
{
    private String propName;   // primary key
    private String propValue;

    public DbApplicationProperty()
    {
    }

    public String getPropName() {
        return propName;
    }

    public void setPropName(String propName) {
        this.propName = propName;
    }

    public String getPropValue() {
        return propValue;
    }

    public void setPropValue(String propValue) {
        this.propValue = propValue;
    }

    public String toString()
    {
        return "Application Property Name: " + getPropName() +
               "   Value: " + getPropValue();
    }
}
