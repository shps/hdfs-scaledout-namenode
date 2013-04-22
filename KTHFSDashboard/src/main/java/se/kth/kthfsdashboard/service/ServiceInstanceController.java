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
   @ManagedProperty("#{param.cluster}")
   private String cluster;
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

      hdfsRoles = new String[]{"namenode", "datanode"};
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

   public void setCluster(String cluster) {
      this.cluster = cluster;
   }

   public String getCluster() {
      return cluster;
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

   public List<InstanceInfo> getInstances() {
      List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
      List<Service> services;

      if (cluster != null && service != null && serviceGroup != null && status != null) {
         services = serviceEJB.findByInstanceGroupServiceStatus(cluster, serviceGroup, service, Service.getServiceStatus(status));
         cookie.write("instance", cluster);
         cookie.write("group", serviceGroup);

      } else if (cluster != null && serviceGroup != null && service != null) {
         services = serviceEJB.findByInstanceGroupService(cluster, serviceGroup, service);
         cookie.write("instance", cluster);
         cookie.write("group", serviceGroup);

      } else if (cluster != null && serviceGroup != null) {
         services = serviceEJB.findByInstanceGroup(cluster, serviceGroup);
         cookie.write("instance", cluster);
         cookie.write("group", serviceGroup);

      } else if (cluster != null) {
         services = serviceEJB.findByInstance(cluster);
         cookie.write("instance", cluster);
         cookie.write("group", serviceGroup);

      } else {
         services = serviceEJB.findByInstanceGroup(cookie.read("instance"), cookie.read("group"));
      }
      for (Service s : services) {                
         instances.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "-", s.getStatus(), s.getHealth().toString()));
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

      if (serviceGroup.equals(Service.ServiceClass.KTHFS.toString())) {
         return hdfsRoleOptions;
      } else if (serviceGroup.equals(Service.ServiceClass.MySQLCluster.toString())) {
         return mysqlclusterRoleOptions;
      } else if (serviceGroup.equals(Service.ServiceClass.YARN.toString())) {
         return yarnRoleOptions;
      } else {
         return new SelectItem[]{};
      }
   }

   public SelectItem[] getHealthOptions() {
      return healthOptions;
   }

   public boolean getShowLogs() {

      if (service != null && service.equalsIgnoreCase("mysqlcluster")) {
         return false;
      }
      return true;
   }

   public boolean getShowConfiguration() {
      
      if (serviceGroup == null) {
         return false;
      }
      if (serviceGroup.toLowerCase().contains("mysqlcluster")) {
         return true;
      }
      return false;
   }
}
