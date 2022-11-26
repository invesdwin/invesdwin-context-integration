package de.invesdwin.context.integration.ws.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@NotThreadSafe
public class XmlRequestMappingHandlerMapping extends RequestMappingHandlerMapping {

    /**
     * Restore being able to define controllers in xml that are not annotated with @Controller, but
     * only @RequestMapping.
     */
    @Override
    protected boolean isHandler(final Class<?> beanType) {
        return super.isHandler(beanType) || AnnotatedElementUtils.hasAnnotation(beanType, RequestMapping.class);
    }

}
