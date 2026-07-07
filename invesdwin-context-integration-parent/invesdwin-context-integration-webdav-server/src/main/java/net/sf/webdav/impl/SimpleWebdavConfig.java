package net.sf.webdav.impl;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.spi.IWebdavConfig;

@NotThreadSafe
public class SimpleWebdavConfig implements IWebdavConfig {

    private boolean lazyCreate;

    private boolean omitContentLength;

    private String alt404;

    private String defaultIndex;

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withLazyFolderCreationOnPut() {
        //CHECKSTYLE:ON
        this.lazyCreate = true;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withoutLazyFolderCreationOnPut() {
        //CHECKSTYLE:ON
        this.lazyCreate = false;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withOmitContentLengthHeader() {
        //CHECKSTYLE:ON
        this.omitContentLength = true;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withoutOmitContentLengthHeader() {
        //CHECKSTYLE:ON
        this.omitContentLength = false;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withAlt404Path(final String path) {
        //CHECKSTYLE:ON
        this.alt404 = path;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withoutAlt404Path() {
        //CHECKSTYLE:ON
        this.alt404 = null;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withDefaultIndex(final String path) {
        //CHECKSTYLE:ON
        this.defaultIndex = path;
        return this;
    }

    //CHECKSTYLE:OFF
    public SimpleWebdavConfig withoutDefaultIndex() {
        //CHECKSTYLE:ON
        this.defaultIndex = null;
        return this;
    }

    @Override
    public boolean isLazyFolderCreationOnPut() {
        return lazyCreate;
    }

    @Override
    public boolean isOmitContentLengthHeaders() {
        return omitContentLength;
    }

    @Override
    public String getAlt404Path() {
        return alt404;
    }

    @Override
    public String getDefaultIndexPath() {
        return defaultIndex;
    }

}
