/*
 * NodeAttributeKey.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class NodeAttributeKey
{
    private String langCode;
    private String attName;


    public NodeAttributeKey()
    {
    }

    public NodeAttributeKey(String langCode, String attName)
    {
        this.langCode = langCode;
        this.attName = attName;
    }


    public String getAttName()
    {
        return attName;
    }

    public void setAttName(String attName)
    {
        this.attName = attName;
    }

    public String getLangCode()
    {
        return langCode;
    }

    public void setLangCode(String langCode)
    {
        this.langCode = langCode;
    }


    public boolean equals(Object obj)
    {
        if (! (obj instanceof NodeAttributeKey)) {
            return false;
        }
        final NodeAttributeKey other = (NodeAttributeKey) obj;
        if ((this.attName == null) ? (other.attName != null) : !this.attName.equals(other.attName)) {
            return false;
        }
        if ((this.langCode == null) ? (other.langCode != null) : !this.langCode.equals(other.langCode)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        int hash = (this.attName != null) ? this.attName.hashCode() : 0;
        hash = 29 * hash + (this.langCode != null ? this.langCode.hashCode() : 0);
        return hash;
    }


}
