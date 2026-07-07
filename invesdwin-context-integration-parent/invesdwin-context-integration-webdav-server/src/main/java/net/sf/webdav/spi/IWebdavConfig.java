package net.sf.webdav.spi;

public interface IWebdavConfig {

    boolean isLazyFolderCreationOnPut();

    boolean isOmitContentLengthHeaders();

    String getAlt404Path();

    String getDefaultIndexPath();

}
