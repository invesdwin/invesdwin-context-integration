package net.sf.webdav.exceptions;

import java.text.MessageFormat;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class WebdavException extends Exception {

    private static final long serialVersionUID = 1L;
    private final Object[] params;
    //CHECKSTYLE:OFF
    private transient String formattedMessage;
    //CHECKSTYLE:ON

    protected WebdavException() {
        this.params = null;
    }

    protected WebdavException(final Throwable cause) {
        super(cause);
        this.params = null;
    }

    public WebdavException(final String message, final Object... params) {
        super(message);
        this.params = params;
    }

    public WebdavException(final String message, final Throwable cause, final Object... params) {
        super(message, cause);
        this.params = params;
    }

    @Override
    public synchronized String getMessage() {
        if (formattedMessage == null) {
            final String format = super.getMessage();
            if (format == null) {
                return null;
            }

            if (params == null || params.length < 1) {
                formattedMessage = format;
            } else {
                final String original = formattedMessage;
                try {
                    //CHECKSTYLE:OFF
                    formattedMessage = String.format(format, params);
                    //CHECKSTYLE:ON
                } catch (final Error e) {
                } catch (final RuntimeException e) {
                } catch (final Exception e) {
                }

                if (formattedMessage == null || original == formattedMessage) {
                    try {
                        formattedMessage = MessageFormat.format(format, params);
                    } catch (final Error e) {
                        formattedMessage = format;
                        throw e;
                    } catch (final RuntimeException e) {
                        formattedMessage = format;
                        throw e;
                    } catch (final Exception e) {
                        formattedMessage = format;
                    }
                }
            }
        }

        return formattedMessage;
    }
}
