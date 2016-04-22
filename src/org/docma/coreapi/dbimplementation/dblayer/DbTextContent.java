/*
 * DbTextContent.java
 */

package org.docma.coreapi.dbimplementation.dblayer;


/**
 *
 * @author MP
 */
public class DbTextContent implements DbNodeEntity
{
    private long textDbId = 0;  // generated value; 0 means transient 
    private DbNode owner;
    private String langCode;
    private String content = null;
    private long contentLength = 0;
    private String contentType = "";

    DbTextContent()
    {
    }


    public long getTextDbId()
    {
        return textDbId;
    }

    void setTextDbId(long txtDbId)
    {
        this.textDbId = txtDbId;
    }

    public boolean persisted()
    {
        return textDbId > 0;
    }

    public String getContent()
    {
        return content;
    }

    public void setContent(String cont)
    {
        this.content = cont;
        this.contentLength = (cont == null) ? 0 : cont.length();
    }


    public long getContentLength()
    {
        return contentLength;
    }

    void setContentLength(long contentLength)
    {
        this.contentLength = contentLength;
    }


    public String getContentType()
    {
        return contentType;
    }

    public void setContentType(String contentType)
    {
        this.contentType = contentType;
    }


    public String getLangCode()
    {
        return langCode;
    }

    public void setLangCode(String langCode)
    {
        this.langCode = langCode;
    }


    public DbNode getOwner()
    {
        return owner;
    }

    void setOwner(DbNode owner)
    {
        this.owner = owner;
    }


}
