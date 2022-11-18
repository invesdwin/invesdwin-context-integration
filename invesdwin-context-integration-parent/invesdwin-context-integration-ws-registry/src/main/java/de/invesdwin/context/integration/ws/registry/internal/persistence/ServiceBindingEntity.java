package de.invesdwin.context.integration.ws.registry.internal.persistence;

import java.net.URI;
import java.util.Date;

import javax.annotation.concurrent.NotThreadSafe;

import org.joda.time.DateTime;

import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.context.persistence.jpa.api.dao.entity.AUnversionedEntity;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.util.time.DateTimes;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@NotThreadSafe
@Table(uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "accessUri" }) })
@Indexes(@Index(columnNames = { "name", "accessUri" }, unique = true))
public class ServiceBindingEntity extends AUnversionedEntity {

    @Column(nullable = false)
    private String name;
    @Column(nullable = false)
    private URI accessUri;
    @Column(nullable = false)
    private Date created;
    @Column(nullable = false)
    private Date updated;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public URI getAccessUri() {
        return accessUri;
    }

    public void setAccessUri(final URI accessUri) {
        this.accessUri = accessUri;
    }

    public DateTime getCreated() {
        return DateTimes.fromDate(created);
    }

    public void setCreated(final DateTime created) {
        this.created = DateTimes.toDate(created);
    }

    public void setCreated(final FDate created) {
        this.created = FDates.toDate(created);
    }

    public DateTime getUpdated() {
        return DateTimes.fromDate(updated);
    }

    public void setUpdated(final DateTime updated) {
        this.updated = DateTimes.toDate(updated);
    }

    public void setUpdated(final FDate updated) {
        this.updated = FDates.toDate(updated);
    }

    public ServiceBinding toServiceBinding() {
        final ServiceBinding serviceBinding = new ServiceBinding();
        serviceBinding.mergeFrom(this);
        return serviceBinding;
    }

    public static ServiceBindingEntity valueOf(final String serviceName, final URI accessUri) {
        final ServiceBindingEntity e = new ServiceBindingEntity();
        e.setName(serviceName);
        e.setAccessUri(accessUri);
        return e;
    }

    public static ServiceBindingEntity valueOf(final String serviceName) {
        final ServiceBindingEntity e = new ServiceBindingEntity();
        e.setName(serviceName);
        return e;
    }

}
