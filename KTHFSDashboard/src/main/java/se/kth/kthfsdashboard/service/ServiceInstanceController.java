package se.kth.kthfsdashboard.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.model.SelectItem;
import se.kth.kthfsdashboard.struct.InstanceInfo;
import se.kth.kthfsdashboard.util.CookieTools;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class ServiceInstanceController implements Serializable {

   @ManagedProperty("#{param.hostname}")
   private String hostname;
   @ManagedProperty("#{param.service}")
   private String service;
   @ManagedProperty("#{param.servicegroup}")
   private String serviceGroup;
   @ManagedProperty("#{param.kthfsinstance}")
   private String kthfsInstance;
   @ManagedProperty("#{param.status}")
   private String status;
   @EJB
   private ServiceEJB serviceEJB;
   List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
   private static Logger log = Logger.getLogger(ServiceInstanceController.class.getName());
   private List<InstanceInfo> filteredInstances;
   private SelectItem[] statusOptions;
   private SelectItem[] hdfsRoleOptions;
   private SelectItem[] healthOptions;
   private SelectItem[] mysqlclusterRoleOptions;
   private SelectItem[] yarnRoleOptions;
   private final static String[] statusStates;
   private final static String[] hdfsRoles;
   private final static String[] mysqlClusterRoles;
   private final static String[] yarnRoles;
   private final static String[] healthStates;
   private CookieTools cookie = new CookieTools();

   static {
      statusStates = new String[3];
      statusStates[0] = Service.Status.Started.toString();
      statusStates[1] = Service.Status.Stopped.toString();
      statusStates[2] = Service.Status.Failed.toString();

      hdfsRoles = new String[]{"namenode", "datanode", "mysqlcluster"};
      mysqlClusterRoles = new String[]{"ndb", "mgmserver", "mysqld"};
      yarnRoles = new String[]{"resourcemanager", "nodemanager"};
      healthStates = new String[]{"Good", "Bad"};
   }

   public ServiceInstanceController() {

      statusOptions = createFilterOptions(statusStates);
      hdfsRoleOptions = createFilterOptions(hdfsRoles);
      mysqlclusterRoleOptions = createFilterOptions(mysqlClusterRoles);
      yarnRoleOptions = createFilterOptions(yarnRoles);
      healthOptions = createFilterOptions(healthStates);
   }

   public String getService() {
      return service;
   }

   public void setService(String service) {
      this.service = service;
   }

   public String getServiceGroup() {
      return serviceGroup;
   }

   public void setServiceGroup(String serviceGroup) {
      this.serviceGroup = serviceGroup;
   }

   public String getHostname() {
      return hostname;
   }

   public void setHostname(String hostname) {
      this.hostname = hostname;
   }

   public void setKthfsInstance(String kthfsInstance) {
      this.kthfsInstance = kthfsInstance;
   }

   public String getKthfsInstance() {
      return kthfsInstance;
   }

   public String getStatus() {
      return status;
   }

   public void setStatus(String status) {
      this.status = status;
   }

   public List<InstanceInfo> getFilteredInstances() {
      return filteredInstances;
   }

   public void setFilteredInstances(List<InstanceInfo> filteredInstances) {
      this.filteredInstances = filteredInstances;
   }

   public List<InstanceInfo> getSubserviceInstances() {
      List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
      List<Service> services;

      if (kthfsInstance != null && serviceGroup != null && service != null && !service.equals(serviceGroup)) {
         services = serviceEJB.findByInstanceServiceGroup(kthfsInstance, serviceGroup, service);
         cookie.write("instance", kthfsInstance);
         cookie.write("servicegroup", serviceGroup);
      } else if (kthfsInstance != null && serviceGroup != null) {
         services = serviceEJB.findByInstanceServiceGroup(kthfsInstance, serviceGroup);
         cookie.write("instance", kthfsInstance);
         cookie.write("servicegroup", serviceGroup);
      } else {
         services = serviceEJB.findByInstanceServiceGroup(cookie.read("instance"), cookie.read("servicegroup"));
      }
      for (Service s : services) {
         instances.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
      }
      return instances;
   }

   public List<InstanceInfo> getInstances() {
      List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
      List<Service> services;

      if (kthfsInstance != null && service != null && status != null) {
         services = serviceEJB.findByInstanceServiceStatus(kthfsInstance, service, Service.getServiceStatus(status));
         cookie.write("instance", kthfsInstance);
      } else if (kthfsInstance != null && service != null) {
         services = serviceEJB.findByInstanceService(kthfsInstance, service);
         cookie.write("instance", kthfsInstance);
      } else if (kthfsInstance != null) {
         services = serviceEJB.findByInstance(kthfsInstance);
         cookie.write("instance", kthfsInstance);
      } else {
         services = serviceEJB.findByInstance(cookie.read("instance"));
      }
      for (Service s : services) {
         instances.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
      }
      return instances;
   }

   private SelectItem[] createFilterOptions(String[] data) {
      SelectItem[] options = new SelectItem[data.length + 1];

      options[0] = new SelectItem("", "Any");
      for (int i = 0; i < data.length; i++) {
         options[i + 1] = new SelectItem(data[i], data[i]);
      }

      return options;
   }

   public SelectItem[] getStatusOptions() {
      return statusOptions;
   }

   public SelectItem[] getRoleOptions() {
      
      Service.ServiceClass serviceClass = serviceEJB.findServiceClass(kthfsInstance);
      if (serviceClass.equals(Service.ServiceClass.KTHFS)) {
         return hdfsRoleOptions;
      } else if (serviceClass.equals(Service.ServiceClass.YARN)) {
         return yarnRoleOptions;
      } else {
         return new SelectItem[]{};
      }
   }

   public SelectItem[] getSubServiceRoleOptions() {

      if (serviceGroup.equalsIgnoreCase("mysqlcluster")) {
         return mysqlclusterRoleOptions;
      } else {
         return new SelectItem[]{};
      }
   }

   public SelectItem[] getHealthOptions() {
      return healthOptions;
   }

   public boolean getShowSubservices() {

      System.err.println(">>>>" + service);
      
      if (service != null && service.equalsIgnoreCase("mysqlcluster")) {
         return true;
      }
      return false;
   }

   public boolean getShoWLogs() {

        if (service != null &&  service.equalsIgnoreCase("mysqlcluster")) {
            return false;
        }
      return true;
   }

   public boolean getShowConfiguration() {
      
      if (service == null) {
         return false;
      }
      if (service.contains("mysqlcluster")) {
         return true;
      }
      return false;
   }
}
