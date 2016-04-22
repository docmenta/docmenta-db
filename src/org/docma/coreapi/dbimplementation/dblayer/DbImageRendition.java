/*
 * DbImageRendition.java
 */

package org.docma.coreapi.dbimplementation.dblayer;


/**
 *
 * @author MP
 */
public class DbImageRendition
{
    private long renditionDbId = 0;   // generated value; 0 means transient
    private DbNode owner;
    private String langCode;
    private byte[] content = null;
    private long contentLength = 0;
    private String contentType = "";
    private String renditionName = "";
    private int maxWidth = 0;
    private int maxHeight = 0;


    DbImageRendition()
    {
    }


    public long getRenditionDbId()
    {
        return renditionDbId;
    }

    void setRenditionDbId(long renditionDbId)
    {
        this.renditionDbId = renditionDbId;
    }


    public byte[] getContent()
    {
        return content;
    }

    public void setContent(byte[] content)
    {
        this.content = content;
        setContentLength(content.length);
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


    public int getMaxHeight()
    {
        return maxHeight;
    }

    public void setMaxHeight(int maxHeight)
    {
        this.maxHeight = maxHeight;
    }


    public int getMaxWidth()
    {
        return maxWidth;
    }

    public void setMaxWidth(int maxWidth)
    {
        this.maxWidth = maxWidth;
    }


    public String getRenditionName()
    {
        return renditionName;
    }

    public void setRenditionName(String renditionName)
    {
        this.renditionName = renditionName;
    }


}
