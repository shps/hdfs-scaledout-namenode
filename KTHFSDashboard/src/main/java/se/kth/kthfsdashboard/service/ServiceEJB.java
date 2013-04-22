package se.kth.kthfsdashboard.service;

import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
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

   public Long findServiceCount(String instance, String serviceGroup, String service) {
      TypedQuery<Long> query = em.createNamedQuery("findServiceCount", Long.class)
              .setParameter("instance", instance)
              .setParameter("serviceGroup", serviceGroup)
              .setParameter("service", service);
      return query.getSingleResult();
   }

   public List<String> findDistinctInstances() {
      TypedQuery<String> query = em.createNamedQuery("findDistinctInstances", String.class);
      return query.getResultList();
   }

   public Service.ServiceClass findServiceClass(String instance) {
      TypedQuery<Service.ServiceClass> query = em.createNamedQuery("findServiceClass", Service.ServiceClass.class)
              .setParameter("instance", instance);
      return query.getResultList().get(0);
   }
   
   public List<String> findServiceGroups(String instance) {
      TypedQuery<String> query = em.createNamedQuery("Service.findServiceGroups", String.class)
              .setParameter("instance", instance);
      return query.getResultList();
   }   

   public List<Service> findAllInstances() {
      TypedQuery<Service> query = em.createNamedQuery("findAllServices", Service.class);
      return query.getResultList();
   }

   public List<Service> findByInstanceServiceStatus(String cluster, String service, Service.Status status) {
      TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", cluster).setParameter("service", service).setParameter("status", status);
      return query.getResultList();
   }
   
   public List<Service> findByInstanceGroupServiceStatus(String cluster, String group, String service, Service.Status status) {
      TypedQuery<Service> query = em.createNamedQuery("Service.findBy-Instance-Group-Service-Status", Service.class).setParameter("instance", cluster).setParameter("group", group).setParameter("service", service).setParameter("status", status);
      return query.getResultList();
   }   
   

   public List<Service> findByInstanceService(String cluster, String service) {
      TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service", Service.class).setParameter("instance", cluster).setParameter("service", service);
      return query.getResultList();
   }
   
   public List<Service> findByInstanceGroupService(String cluster, String group, String service) {
      TypedQuery<Service> query = em.createNamedQuery("Service.findBy-Instance-Group-Service", Service.class).setParameter("instance", cluster).setParameter("group", group).setParameter("service", service);
      return query.getResultList();
   }   

   public List<String> findHostnameByInstanceGroupService(String cluster, String group, String service) {
      TypedQuery<String> query = em.createNamedQuery("Service.findHostnameBy-Instance-Group-Service", String.class).setParameter("instance", cluster).setParameter("group", group).setParameter("service", service);
      return query.getResultList();
   }   
   
   
   public List<Service> findByInstance(String cluster) {
      TypedQuery<Service> query = em.createNamedQuery("Service.findServiceBy-Instance", Service.class).setParameter("instance", cluster);
      return query.getResultList();
   }

   public List<Service> findByInstanceGroup(String cluster, String group) {
      TypedQuery<Service> query = em.createNamedQuery("Service.findBy-Instance-Group", Service.class).setParameter("instance", cluster).setParameter("group", group);
      return query.getResultList();
   }
      
   public List<String> findDistinctServiceGroupByInstance(String cluster) {
      TypedQuery<String> query = em.createNamedQuery("Service.findDistinctServiceGroupBy-Instance", String.class).setParameter("instance", cluster);
      return query.getResultList();
   }   

//   public List<Service> findSubserviceByInstance(String cluster) {
//      TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance", Service.class).setParameter("instance", cluster);
//      return query.getResultList();
//   }

   public List<Service> findInstances(String cluster, String host, String service) {
      TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-Service-Hostname", Service.class).setParameter("hostname", host).setParameter("service", service).setParameter("instance", cluster);
      return query.getResultList();
   }

   public List<Service> findServiceByInstanceServiceGroup(String cluster, String serviceGroup) {
      TypedQuery<Service> query = em.createNamedQuery("findServiceBy-Instance-ServiceGroup", Service.class).setParameter("serviceGroup", serviceGroup).setParameter("instance", cluster);
      return query.getResultList();
   }

//   public List<Service> findByInstanceServiceGroup(String cluster, String serviceGroup) {
//      TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance-ServiceGroup", Service.class).setParameter("instance", cluster).setParameter("serviceGroup", serviceGroup);
//      return query.getResultList();
//   }
//
//   public List<Service> findByInstanceServiceGroup(String cluster, String serviceGroup, String service) {
//      TypedQuery<Service> query = em.createNamedQuery("findSubserviceBy-Instance-ServiceGroup-Service", Service.class).setParameter("instance", cluster).setParameter("serviceGroup", serviceGroup).setParameter("service", service);
//      return query.getResultList();
//   }

   public Service findService(String hostname, String cluster, String serviceGroup, String service) {
      TypedQuery<Service> query = em.createNamedQuery("findService", Service.class).setParameter("hostname", hostname).setParameter("instance", cluster).setParameter("serviceGroup", serviceGroup).setParameter("service", service);
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

   public int getServicesStatusCount(String instance, String group, String service, Service.Status status) {
      TypedQuery<Service> query;
      query = em.createNamedQuery("Service.findBy-Instance-Group-Service-Status", Service.class).setParameter("instance", instance).setParameter("group", group).setParameter("service", service).setParameter("status", status);
      return query.getResultList().size();
   } 
      
//   public int getStartedServicesCount(String instance, String service, boolean subService) {
//      TypedQuery<Service> query;
//
//      if (subService) {
//         query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Started);
//      } else {
//         query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Started);
//      }
//      return query.getResultList().size();
//   }
//
//   public int getStoppedServicesCount(String instance, String service, boolean subService) {
//      TypedQuery<Service> query;
//      if (subService) {
//         query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Stopped);
//      } else {
//         query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Stopped);
//      }
//      return query.getResultList().size();
//   }
//
//   public int getFailedServicesCount(String instance, String service, boolean subService) {
//      TypedQuery<Service> query;
//      if (subService) {
//         query = em.createNamedQuery("findSubserviceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Failed);
//      } else {
//         query = em.createNamedQuery("findServiceBy-Instance-Service-Status", Service.class).setParameter("instance", instance).setParameter("service", service).setParameter("status", Service.Status.Failed);
//      }
//      return query.getResultList().size();
//   }
}
