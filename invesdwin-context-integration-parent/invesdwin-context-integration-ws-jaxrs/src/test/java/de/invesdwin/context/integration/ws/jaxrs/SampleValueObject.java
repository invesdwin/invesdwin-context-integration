package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.bean.AValueObject;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@NotThreadSafe
public class SampleValueObject extends AValueObject {

    private String getIt;

    public String getGetIt() {
        return getIt;
    }

    public void setGetIt(final String getIt) {
        this.getIt = getIt;
    }

}
