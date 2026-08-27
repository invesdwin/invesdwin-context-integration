package de.invesdwin.context.integration.webdav.server.internal;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.catalina.servlets.ExtendedWebdavServlet;
import org.apache.catalina.servlets.FakeCatalinaContext;
import org.apache.catalina.servlets.FakeCatalinaWebdavResourceRoot;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import jakarta.servlet.ServletException;

@Configurable
@Immutable
public class ConfiguredWebdavServlet extends ExtendedWebdavServlet {

    @Override
    public void init() throws ServletException {
        if (getServletContext().getAttribute(org.apache.catalina.Globals.RESOURCES_ATTR) == null) {
            final FakeCatalinaContext context = new FakeCatalinaContext(getServletContext());
            final FakeCatalinaWebdavResourceRoot resources = new FakeCatalinaWebdavResourceRoot(context,
                    WebdavServerProperties.WORKING_DIRECTORY.toPath());
            getServletContext().setAttribute(org.apache.catalina.Globals.RESOURCES_ATTR, resources);
        }
        super.init();
        showServerInfo = false;
        readOnly = false;
        listings = true;
    }

    @Override
    @SkipParallelExecution
    @Scheduled(fixedDelay = 10, timeUnit = TimeUnit.SECONDS)
    public void periodicEvent() {
        super.periodicEvent();
    }

}