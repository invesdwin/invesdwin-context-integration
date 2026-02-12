package de.invesdwin.context.integration.webdav.server.internal.impl;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import jakarta.servlet.http.HttpServletRequest;
import net.sf.webdav.spi.WebdavRequest;

@NotThreadSafe
public class ServletWebdavRequest implements WebdavRequest {

    private final HttpServletRequest req;

    public ServletWebdavRequest(final HttpServletRequest req) {
        this.req = req;
    }

    @Override
    public String getMethod() {
        return req.getMethod();
    }

    @Override
    public Principal getUserPrincipal() {
        return req.getUserPrincipal();
    }

    @Override
    public String getRequestURI() {
        return req.getRequestURL().toString();
    }

    @Override
    public Set<String> getHeaderNames() {
        final ArrayList<String> list = Collections.list(req.getHeaderNames());
        return ILockCollectionFactory.getInstance(false).newSet(list);
    }

    @Override
    public String getHeader(final String name) {
        return req.getHeader(name);
    }

    @Override
    public Set<String> getAttributeNames() {
        final ArrayList<String> list = Collections.list(req.getAttributeNames());
        return ILockCollectionFactory.getInstance(false).newSet(list);
    }

    @Override
    public String getAttribute(final String name) {
        final Object val = req.getAttribute(name);
        return val == null ? null : val.toString();
    }

    @Override
    public Set<String> getParameterNames() {
        final ArrayList<String> list = Collections.list(req.getParameterNames());
        return ILockCollectionFactory.getInstance(false).newSet(list);
    }

    @Override
    public String getParameter(final String name) {
        return req.getParameter(name);
    }

    @Override
    public String getPathInfo() {
        return req.getPathInfo();
    }

    @Override
    public Locale getLocale() {
        return req.getLocale();
    }

    @Override
    public String getServerName() {
        return req.getServerName();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return req.getInputStream();
    }

    @Override
    public int getContentLength() {
        return req.getContentLength();
    }

    @Override
    public String getContextPath() {
        return req.getContextPath();
    }

    @Override
    public String getServicePath() {
        return req.getServletPath();
    }

}
