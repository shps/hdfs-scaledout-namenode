package se.kth.kthfsdashboard.service;

import se.kth.kthfsdashboard.role.RoleEJB;
import se.kth.kthfsdashboard.role.Role;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.model.SelectItem;
import se.kth.kthfsdashboard.role.Role.RoleType;
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
   @ManagedProperty("#{param.role}")
   private String role;
   @ManagedProperty("#{param.servicegroup}")
   private String serviceGroup;
   @ManagedProperty("#{param.cluster}")
   private String cluster;
   @ManagedProperty("#{param.status}")
   private String status;
   @EJB
   private RoleEJB roleEjb;
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
      statusStates[0] = Role.Status.Started.toString();
      statusStates[1] = Role.Status.Stopped.toString();
      statusStates[2] = Role.Status.Failed.toString();

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

   public String getRole() {
      return role;
   }

   public void setRole(String role) {
      this.role = role;
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
      List<Role> roles;
      
      if (cluster != null && role != null && serviceGroup != null && status != null) {
         roles = roleEjb.findRoles(cluster, serviceGroup, role, Role.getServiceStatus(status));
         cookie.write("cluster", cluster);
         cookie.write("service", serviceGroup);

      } else if (cluster != null && serviceGroup != null && role != null) {
         roles = roleEjb.findRoles(cluster, serviceGroup, role);
         cookie.write("cluster", cluster);
         cookie.write("service", serviceGroup);

      } else if (cluster != null && serviceGroup != null) {
         roles = roleEjb.findRoles(cluster, serviceGroup);
         cookie.write("cluster", cluster);
         cookie.write("service", serviceGroup);

      } else if (cluster != null) {
         roles = roleEjb.findRoles(cluster);
         cookie.write("cluster", cluster);
         cookie.write("service", serviceGroup);

      } else {
         roles = roleEjb.findRoles(cookie.read("cluster"), cookie.read("service"));
      }
      for (Role r : roles) {                
         instances.add(new InstanceInfo(r.getCluster(), r.getServiceGroup(), r.getRole(), r.getHostname(), "-", r.getStatus(), r.getHealth().toString()));
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

      if (serviceGroup.equals(ServiceType.KTHFS.toString())) {
         return hdfsRoleOptions;
      } else if (serviceGroup.equals(ServiceType.MySQLCluster.toString())) {
         return mysqlclusterRoleOptions;
      } else if (serviceGroup.equals(ServiceType.YARN.toString())) {
         return yarnRoleOptions;
      } else {
         return new SelectItem[]{};
      }
   }

   public SelectItem[] getHealthOptions() {
      return healthOptions;
   }

   public boolean getShowConfiguration() {
      
      if (serviceGroup == null) {
         return false;
      }
      if (serviceGroup.equalsIgnoreCase(ServiceType.MySQLCluster.toString())) {
         return true;
      }
      return false;
   }
}
