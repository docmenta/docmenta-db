/*
 * ImageRenditionKey.java
 */

package org.docma.coreapi.dbimplementation.dblayer;

/**
 *
 * @author MP
 */
public class ImageRenditionKey
{
    private String langCode;
    private String renditionName;

    public ImageRenditionKey()
    {
    }

    public ImageRenditionKey(String langCode, String renditionName)
    {
        this.langCode = langCode;
        this.renditionName = renditionName;
    }


    public String getRenditionName()
    {
        return renditionName;
    }

    public void setRenditionName(String renditionName)
    {
        this.renditionName = renditionName;
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
        if (! (obj instanceof ImageRenditionKey)) {
            return false;
        }
        final ImageRenditionKey other = (ImageRenditionKey) obj;
        if ((this.renditionName == null) ? (other.renditionName != null) : !this.renditionName.equals(other.renditionName)) {
            return false;
        }
        if ((this.langCode == null) ? (other.langCode != null) : !this.langCode.equals(other.langCode)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        int hash = (this.renditionName != null) ? this.renditionName.hashCode() : 0;
        hash = 29 * hash + (this.langCode != null ? this.langCode.hashCode() : 0);
        return hash;
    }

}
