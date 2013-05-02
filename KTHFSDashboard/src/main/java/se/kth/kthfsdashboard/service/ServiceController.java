package se.kth.kthfsdashboard.service;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;
import javax.servlet.http.HttpServletRequest;
import se.kth.kthfsdashboard.host.HostEJB;
import se.kth.kthfsdashboard.struct.InstanceFullInfo;
import se.kth.kthfsdashboard.struct.InstanceInfo;
import se.kth.kthfsdashboard.struct.ClusterInfo;
import se.kth.kthfsdashboard.struct.ServiceRoleInfo;
import se.kth.kthfsdashboard.util.Formatter;
import se.kth.kthfsdashboard.util.WebCommunication;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class ServiceController {

   @EJB
   private HostEJB hostEJB;
   @EJB
   private ServiceEJB serviceEJB;
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
   private HashMap<String, ClusterInfo> clusters = new HashMap<String, ClusterInfo>();
   private HashMap<String, InstanceInfo> instances = new HashMap<String, InstanceInfo>();
   private static Logger log = Logger.getLogger(ServiceController.class.getName());
   public static String NOT_AVAILABLE = "Not available.";
   public static Map<String, List<ServiceRoleInfo>> rolesMap = new HashMap<String, List<ServiceRoleInfo>>();
   public static Map<String, String> servicesRolesMap = new HashMap<String, String>();

   
   public ServiceController() {

      List<ServiceRoleInfo> roles;

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("NameNode", "namenode"));
      roles.add(new ServiceRoleInfo("DataNode", "datanode"));
      rolesMap.put(Service.ServiceClass.KTHFS.toString(), roles);

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("MySQL Cluster NDBD (ndb)", "ndb"));
      roles.add(new ServiceRoleInfo("MySQL Server (mysqld)", "mysqld"));
      roles.add(new ServiceRoleInfo("MGM Server (mgmserver)", "mgmserver"));
      rolesMap.put(Service.ServiceClass.MySQLCluster.toString(), roles);

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("Resource Manager", "resourcemanager"));
      roles.add(new ServiceRoleInfo("Node Manager", "nodemanager"));
      rolesMap.put(Service.ServiceClass.YARN.toString(), roles);
      
      servicesRolesMap.put("namenode", Service.ServiceClass.KTHFS.toString());
      servicesRolesMap.put("datanode", Service.ServiceClass.KTHFS.toString());
      
      servicesRolesMap.put("ndb", Service.ServiceClass.MySQLCluster.toString());
      servicesRolesMap.put("mysqld", Service.ServiceClass.MySQLCluster.toString());      
      servicesRolesMap.put("mgmserver", Service.ServiceClass.MySQLCluster.toString());      

      servicesRolesMap.put("resourcemanager", Service.ServiceClass.YARN.toString());
      servicesRolesMap.put("nodemanager", Service.ServiceClass.YARN.toString());
      
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

   public void setStatus(String status) {
      this.status = status;
   }

   public String getStatus() {
      return status;
   }

   public List<ClusterInfo> getClusters() {

      List<ClusterInfo> allClusters = new ArrayList<ClusterInfo>();

      // TODO: Insert correct Infor for Service Types, ...
      // service instances

      List<String> instances = serviceEJB.findDistinctInstances();
      for (String instance : instances) {


         ClusterInfo instanceInfo = new ClusterInfo(instance, serviceEJB.findServiceClass(instance).toString(), "-", "-");

         List<Service> services = serviceEJB.findByInstance(instance);
         for (Service s : services) {
            if (instanceInfo.getRoleCounts().containsKey(s.getService())) {
               Integer count = (Integer) instanceInfo.getRoleCounts().get(s.getService());
               instanceInfo.putToRoleCounts(s.getService(), count + 1);
            } else {
               instanceInfo.putToRoleCounts(s.getService(), 1);
            }
         }

         List<String> serviceGroups = serviceEJB.findDistinctServiceGroupByInstance(instance);
         instanceInfo.setServices(serviceGroups);

         allClusters.add(instanceInfo);
      }
      return allClusters;
   }

   public String requestParams() {
      FacesContext context = FacesContext.getCurrentInstance();
      HttpServletRequest request = (HttpServletRequest) context.getExternalContext().getRequest();
      Principal principal = request.getUserPrincipal();

      return request.getAuthType().toString() + " - " + principal.getName();
   }

   public List<InstanceInfo> getInstances() {
      List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
      List<Service> services = serviceEJB.findAllInstances();
      for (Service s : services) {
         instances.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
      }
      return instances;
   }

   public List<InstanceInfo> getInstance() {
      List<InstanceInfo> instanceInfoList = new ArrayList<InstanceInfo>();
      List<Service> services = serviceEJB.findInstances(cluster, hostname, service);
      for (Service s : services) {
         instanceInfoList.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
      }
      return instanceInfoList;
   }

   public List<InstanceFullInfo> getInstanceFullInfo() {
      List<InstanceFullInfo> instanceInfoList = new ArrayList<InstanceFullInfo>();
      List<Service> services = serviceEJB.findInstances(cluster, hostname, service);
      for (Service s : services) {
         String ip = hostEJB.findHostByName(hostname).getIp();
         InstanceFullInfo i = new InstanceFullInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), ip, s.getWebPort(), "?", s.getStatus(), s.getHealth().toString());
         i.setPid(s.getPid());
         i.setUptime(Formatter.time(s.getUptime() * 1000));
         instanceInfoList.add(i);
      }
      return instanceInfoList;
   }

   public String doGotoClusterStatus() {
      return "cluster-status?faces-redirect=true&cluster=" + cluster;
   }   

   public String gotoServiceInstance() {
      return "services-instances-status?faces-redirect=true&hostname="
              + hostname + "&cluster=" + cluster + "&service=" + service;
   }

   public String gotoClusterStatus() {
      return "cluster-status?faces-redirect=true&cluster=" + cluster;
   }   

   public String gotoClusterCommands() {
      return "cluster-commands?faces-redirect=true&cluster=" + cluster;
   }   
   
   public String gotoServiceStatus() {
      return "service-status?faces-redirect=true&cluster=" + cluster + "&servicegroup=" + serviceGroup;
   }
   
   public String gotoServiceInstances() {     
      String url = "service-instances?faces-redirect=true";
      if (hostname != null) {
         url += "&hostname=" + hostname;
      }
      if (cluster != null) {
         url += "&cluster=" + cluster;
      }
      if (serviceGroup != null) {
         url += "&servicegroup=" + serviceGroup;
      }
      if (service != null) {
         url += "&service=" + service;
      }
      if (status != null) {
         url += "&status=" + status;
      }
      return url;      
   }
   
   public String gotoServiceCommands() {
      return "service-commands?faces-redirect=true&cluster=" + cluster + "&servicegroup=" + serviceGroup;
   }   
   
   public String gotoRole() {
      return "role?faces-redirect=true&hostname=" + hostname + "&cluster=" + cluster +
              "&servicegroup=" + serviceGroup + "&service=" + service;
   }

   public String getRoleUrl() {
      return "role.xhtml";
   }   

   public String getHostUrl() {
      return "host.xhtml";
   }

   public List<ServiceRoleInfo> getServiceRoles() {

      List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();
      Service.ServiceClass serviceClass = serviceEJB.findServiceClass(cluster);
      for (ServiceRoleInfo role : rolesMap.get(serviceClass.toString())) {
         serviceRoles.add(setStatus(cluster, serviceGroup, role));
      }
      return serviceRoles;
   }
   
   public List<ServiceRoleInfo> getRoles() {

      List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();     
      for (ServiceRoleInfo role : rolesMap.get(serviceGroup)) {
         serviceRoles.add(setStatus(cluster, serviceGroup, role));
      }
      return serviceRoles;
   }   
   
   public List<String> getServiceGroups() {

      List<String> serviceRoles = new ArrayList<String>();
      for (String s : serviceEJB.findServiceGroups(cluster)) {         
         serviceRoles.add(s);
      }
      return serviceRoles;
   }   

   public List<ServiceRoleInfo> getSuberviceRoles() {

      List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();
      if (serviceGroup != null) { // serviceGroup = mysqlcluster
         for (ServiceRoleInfo role : rolesMap.get(serviceGroup)) {
            serviceRoles.add(setStatus(cluster, serviceGroup ,role));
         }
      }
      return serviceRoles;
   }

   private ServiceRoleInfo setStatus(String cluster, String group, ServiceRoleInfo role) {
      int started, stopped, failed, good, bad;
      started = serviceEJB.getServicesStatusCount(cluster, group, role.getShortName(), Service.Status.Started);
      stopped = serviceEJB.getServicesStatusCount(cluster, group, role.getShortName(), Service.Status.Stopped);
      failed = serviceEJB.getServicesStatusCount(cluster, group, role.getShortName(), Service.Status.Failed);
//      good = started + stopped;
//      bad = failed;
      good = started;
      bad = failed + stopped;
      role.setStatusStarted(started + " Started");
      role.setStatusStopped((stopped + failed) + " Stopped");
      role.setHealth(String.format("%d Good, %d Bad", good, bad));
      return role;
   }

   public void addMessage(String summary) {
      FacesMessage message = new FacesMessage(FacesMessage.SEVERITY_INFO, summary, null);
      FacesContext.getCurrentInstance().addMessage(null, message);
   }

   public void startService() {
      addMessage("Start not implemented!");
   }

   public void stopService() {
      addMessage("Stop not implemented!");
   }

   public void restartService() {
      addMessage("Restart not implemented!");
   }

   public void deleteService() {
      addMessage("Delete not implemented!");
   }

   public boolean hasWebUi() {
      Service s = serviceEJB.findService(hostname, cluster, serviceGroup, service);
      if (s.getWebPort() == null || s.getWebPort() == 0) {
         return false;
      }
      return true;
   }

   public String getRoleLog(int lines) {
      WebCommunication webComm = new WebCommunication(hostname, cluster, service);
      return webComm.getRoleLog(lines);
   }
   
   public String getAgentLog(int lines) {
      WebCommunication webComm = new WebCommunication(hostname);
      return webComm.getAgentLog(lines);
   }   
      
   public String getMySQLClusterConfig() throws Exception {
      
      // Finds hostname of mgmserver
      // Role=mgmserver , Service=MySQLCluster, Cluster=cluster
      final String SERVICE = "mgmserver";
      String host = serviceEJB.findByInstanceGroupService(cluster, serviceGroup, SERVICE).get(0).getHostname();
      WebCommunication webComm = new WebCommunication(host, cluster, SERVICE);
      return webComm.getConfig();
   }

   public String getNotAvailable() {
      return NOT_AVAILABLE;
   }

   public boolean getShowNdbInfo() {
      if (serviceGroup == null) {
         return false;
      }
      if (serviceGroup.equalsIgnoreCase("mysqlcluster")) {
         return true;
      }      
      return false;
   }
   
   public boolean showKTHFSGraphs() {
      if (serviceGroup.equals(Service.ServiceClass.KTHFS.toString())) {
         return true;
      }
      return false;      
   }
   
   public boolean showMySQLClusterGraphs() {
      if (serviceGroup.equals(Service.ServiceClass.MySQLCluster.toString())) {
         return true;
      }
      return false;      
   }   

   public boolean showNamenodeGraphs() {
      if (service.equals("namenode")) {
         return true;
      }
      return false;
   }
   
   public boolean roleHasGraphs() {
      if (service == null) {
         return false;
      }
      if (service.equals("datanode") || service.equals("namenode")) {
         return true;
      }      
      return false;      
   }

   public boolean showDatanodeGraphs() {
      if (service.equals("datanode")) {
         return true;
      }
      return false;
   }
   
   public String findServiceByRole(String role) {
      return servicesRolesMap.get(role);
   }
}