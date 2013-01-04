package se.kth.kthfsdashboard.service;

import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.Parameter;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Stateless
public class ServiceEJB {

    @PersistenceContext(unitName = "kthfsPU")
    private EntityManager em;

    public ServiceEJB() {
    }

    public List<Service> findAllInstances() {

        TypedQuery<Service> query = em.createNamedQuery("findAllServices", Service.class);
        return query.getResultList();
    }

    public List<Service> findByInstanceServiceStatus(String kthfsInstance, String service, Service.Status status) {

        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", kthfsInstance).setParameter("service", service).setParameter("status", status);
        return query.getResultList();
    }

    public List<Service> findByInstanceService(String kthfsInstance, String service) {

        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service", Service.class).setParameter("instance", kthfsInstance).setParameter("service", service);
        return query.getResultList();
    }

    public List<Service> findByInstance(String kthfsInstance) {

        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance", Service.class).setParameter("instance", kthfsInstance);
        return query.getResultList();
    }

    public List<Service> findSubserviceByInstance(String kthfsInstance) {

        TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance", Service.class).setParameter("instance", kthfsInstance);
        return query.getResultList();
    }

    public List<Service> findInstances(String kthfsInstance, String host, String service) {

        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Hostname", Service.class).setParameter("hostname", host).setParameter("service", service).setParameter("instance", kthfsInstance);
        return query.getResultList();
    }

    
    public List<Service> findServiceByInstanceServiceGroup(String kthfsInstance, String serviceGroup) {

        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-ServiceGroup", Service.class).setParameter("serviceGroup", serviceGroup).setParameter("instance", kthfsInstance);
        return query.getResultList();
    }
    
    public List<Service> findByInstanceServiceGroup(String kthfsInstance, String serviceGroup) {

        TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance-ServiceGroup", Service.class).setParameter("instance", kthfsInstance).setParameter("serviceGroup", serviceGroup);
        return query.getResultList();
    }

    public List<Service> findByInstanceServiceGroup(String kthfsInstance, String serviceGroup, String service) {

        TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance-ServiceGroup-Service", Service.class).setParameter("instance", kthfsInstance).setParameter("serviceGroup", serviceGroup).setParameter("service", service);
        return query.getResultList();
    }
    
    public Service findServices(String hostname, String kthfsInstance, String serviceGroup, String service) {

        TypedQuery<Service> query = em.createNamedQuery("findService", Service.class).setParameter("hostname", hostname).setParameter("instance", kthfsInstance).setParameter("serviceGroup", serviceGroup).setParameter("service", service);
        return query.getSingleResult();
    }    

    public void persistService(Service service) {
        em.persist(service);
    }

    public void storeService(Service service) {
        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Hostname", Service.class).setParameter("hostname", service.getHostname()).setParameter("instance", service.getInstance()).setParameter("service", service.getService());
        List<Service> s = query.getResultList();

        if (s.size() > 0) {
            service.setId(s.get(0).getId());
            em.merge(service);
        } else {
            em.persist(service);
        }

    }

    public void deleteService(Service service) {
        TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Hostname", Service.class).setParameter("hostname", service.getHostname()).setParameter("instance", service.getInstance()).setParameter("service", service.getService());
        Service s = query.getSingleResult();
        em.remove(s);
    }

    public int getStartedServicesCount(String instance, String service, boolean subService) {
        TypedQuery<Service> query;

        if (subService) {
            query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Started);
        } else {
            query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Started);
        }
        return query.getResultList().size();
    }

    public int getStoppedServicesCount(String instance, String service, boolean subService) {

        TypedQuery<Service> query;
        if (subService) {
            query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Stopped);
        } else {
            query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Stopped);
        }
        return query.getResultList().size();
    }

    public int getFailedServicesCount(String instance, String service, boolean subService) {
        TypedQuery<Service> query;
        if (subService) {
            query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Failed);
        } else {
            query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Failed);
        }
        return query.getResultList().size();
    }
}
