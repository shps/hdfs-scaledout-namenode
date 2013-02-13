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
import se.kth.kthfsdashboard.struct.KthfsInstanceInfo;
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
   @ManagedProperty("#{param.kthfsinstance}")
   private String kthfsInstance;
   @ManagedProperty("#{param.status}")
   private String status;
   @ManagedProperty("#{param.jmxparam}")
   private String jmxParam;
   private HashMap<String, KthfsInstanceInfo> kthfsInstances = new HashMap<String, KthfsInstanceInfo>();
   private HashMap<String, InstanceInfo> instances = new HashMap<String, InstanceInfo>();
   private static Logger log = Logger.getLogger(ServiceController.class.getName());
   public static String NOT_AVAILABLE = "Not available.";
   public static Map<String, List<ServiceRoleInfo>> rolesMap = new HashMap<String, List<ServiceRoleInfo>>();

   public ServiceController() {

      List<ServiceRoleInfo> roles;

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("MySQL Cluster", "mysqlcluster"));
      roles.add(new ServiceRoleInfo("NameNode", "namenode"));
      roles.add(new ServiceRoleInfo("DataNode", "datanode"));
      rolesMap.put(Service.ServiceClass.KTHFS.toString(), roles);

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("MySQL Cluster NDBD (ndb)", "ndb"));
      roles.add(new ServiceRoleInfo("MySQL Server (mysqld)", "mysqld"));
      roles.add(new ServiceRoleInfo("MGM Server (mgmserver)", "mgmserver"));
      rolesMap.put("mysqlcluster", roles);

      roles = new ArrayList<ServiceRoleInfo>();
      roles.add(new ServiceRoleInfo("Resource Manager", "resourcemanager"));
      roles.add(new ServiceRoleInfo("Node Manager", "nodemanager"));
      rolesMap.put(Service.ServiceClass.YARN.toString(), roles);
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

   public void setStatus(String status) {
      this.status = status;
   }

   public String getStatus() {
      return status;
   }

   public void setJmxParam(String jmxParam) {
      this.jmxParam = jmxParam;
   }

   public String getJmxParam() {
      return jmxParam;
   }

   public List<KthfsInstanceInfo> getKthfsInstances() {

      List<KthfsInstanceInfo> allKthfsInstances = new ArrayList<KthfsInstanceInfo>();


      // TODO: Insert correct Infor for Service Types, ...
      // service instances

      List<String> instances = serviceEJB.findDistinctInstances();
      for (String instance : instances) {
         
         
         KthfsInstanceInfo instanceInfo = new KthfsInstanceInfo(instance, serviceEJB.findServiceClass(instance).toString(), "?", "?");
         
         List<Service> services = serviceEJB.findByInstance(instance);
         for (Service s: services) {
            if (instanceInfo.getRoleCounts().containsKey(s.getService())) {
               Integer count = (Integer) instanceInfo.getRoleCounts().get(s.getService());
                instanceInfo.putToRoleCounts(s.getService(), count + 1);
            } else {
               instanceInfo.putToRoleCounts(s.getService(), 1);
            }
         }
         
         allKthfsInstances.add(instanceInfo);
      }
      return allKthfsInstances;
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
      List<Service> services = serviceEJB.findInstances(kthfsInstance, hostname, service);
      for (Service s : services) {
         instanceInfoList.add(new InstanceInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
      }
      return instanceInfoList;
   }

   public List<InstanceFullInfo> getInstanceFullInfo() {
      List<InstanceFullInfo> instanceInfoList = new ArrayList<InstanceFullInfo>();
      List<Service> services = serviceEJB.findInstances(kthfsInstance, hostname, service);
      for (Service s : services) {
         InstanceFullInfo i = new InstanceFullInfo(s.getInstance(), s.getServiceGroup(), s.getService(), s.getHostname(), s.getWebPort(), "?", s.getStatus(), s.getHealth().toString());
         i.setPid(s.getPid());
         i.setUptime(Formatter.time(s.getUptime() * 1000));
         instanceInfoList.add(i);
      }
      return instanceInfoList;
   }

   public String doGotoService() {

//        FacesContext context = FacesContext.getCurrentInstance();
//        String kthfsInstance = context.getApplication().evaluateExpressionGet(context, "#{kthfsInstance.name}", String.class);

      return "services-status?faces-redirect=true&kthfsinstance=" + kthfsInstance;
   }

   public String doGotoService(String kthfsInstance) {
      return "services-status?faces-redirect=true&kthfsinstance=" + kthfsInstance;
   }

   public String gotoServiceInstance() {
      return "services-instances-status?faces-redirect=true&hostname="
              + hostname + "&kthfsinstance=" + kthfsInstance + "&service=" + service;
   }

   public String gotoServiceStatus() {
      return "services-status?faces-redirect=true&kthfsinstance=" + kthfsInstance;
   }

   public String gotoServiceCommands() {
      return "services-commands?faces-redirect=true&kthfsinstance=" + kthfsInstance;
   }

   public String gotoServiceInstanceStatus() {
      return "services-instances-status?faces-redirect=true&kthfsinstance="
              + kthfsInstance + "&hostname=" + hostname + "&service=" + service
              + "&servicegroup=" + serviceGroup;
   }

   public String gotoParentServiceInstanceStatus() {
      String parentHostname = "";
      if (serviceGroup.equalsIgnoreCase("mysqlcluster")) {
         //There is one mysqlcluster per kthfs instance
         Service s = serviceEJB.findServiceByInstanceServiceGroup(kthfsInstance, serviceGroup).get(0);
         parentHostname = s.getHostname();
      }
      return "services-instances-status?faces-redirect=true&kthfsinstance="
              + kthfsInstance + "&hostname=" + parentHostname + "&service=" + serviceGroup
              + "&servicegroup=" + serviceGroup;
   }

   public String gotoServiceInstanceSubservices() {
      String url = "services-instances-subservices?faces-redirect=true&kthfsinstance="
              + kthfsInstance + "&hostname=" + hostname;
      if (service != null) {
         url += "&service=" + service;
      }
      if (serviceGroup != null) {
         url += "&servicegroup=" + serviceGroup;
      }
      return url;
   }

   public String gotoServiceInstanceSubservicesInstance() {
      return "services-instances-subservices-instance?faces-redirect=true&kthfsinstance="
              + kthfsInstance + "&hostname=" + hostname + "&service=" + service + "&servicegroup=" + serviceGroup;
   }

   public String getServiceInstanceStatusUrl() {
      return "services-instances-status.xhtml";
   }

   public String getServiceInstanceSubserviceInstanceUrl() {
      return "services-instances-subservices-instance.xhtml";
   }

   public String getHostUrl() {
      return "host.xhtml";
   }

   public String gotoServiceInstances() {
      String url = "services-instances?faces-redirect=true";
      if (hostname != null) {
         url += "&hostname=" + hostname;
      }
      if (kthfsInstance != null) {
         url += "&kthfsinstance=" + kthfsInstance;
      }
      if (service != null) {
         url += "&service=" + service;
      }
      if (status != null) {
         url += "&status=" + status;
      }
      return url;
   }

   public String gotoSubserviceInstances() {

      String url = "services-instances-subservices?faces-redirect=true";
      if (hostname != null) {
         url += "&hostname=" + hostname;
      }
      if (kthfsInstance != null) {
         url += "&kthfsinstance=" + kthfsInstance;
      }
      if (service != null) {
         url += "&service=" + service;
      }
      url += "&servicegroup=" + serviceGroup;
      return url;
   }

   public List<ServiceRoleInfo> getServiceRoles() {

      List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();
      Service.ServiceClass serviceClass = serviceEJB.findServiceClass(kthfsInstance);
      for (ServiceRoleInfo role : rolesMap.get(serviceClass.toString())) {
         serviceRoles.add(setStatus(kthfsInstance, role, false));
      }
      return serviceRoles;
   }

   public List<ServiceRoleInfo> getSuberviceRoles() {

      List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();
      if (serviceGroup != null) { // serviceGroup = mysqlcluster
         for (ServiceRoleInfo role : rolesMap.get(serviceGroup)) {
         serviceRoles.add(setStatus(kthfsInstance, role, true));
         }
      }
      return serviceRoles;
   }

   private ServiceRoleInfo setStatus(String kthfsInstance, ServiceRoleInfo role, boolean isSubSevice) {
      int started, stopped, failed, good, bad;
      started = getStartedServiceCount(kthfsInstance, role.getShortName(), isSubSevice);
      stopped = getStoppedServiceCount(kthfsInstance, role.getShortName(), isSubSevice);
      failed = getFailedServiceCount(kthfsInstance, role.getShortName(), isSubSevice);
      good = started + stopped;
      bad = failed;
      role.setStatusStarted(started + " Started");
      role.setStatusStopped((stopped + failed) + " Stopped");
      role.setHealth(String.format("%d Good, %d Bad", good, bad));
      return role;
   }

   public int getStartedServiceCount(String kthfsInstance, String service, boolean subService) {
      return serviceEJB.getStartedServicesCount(kthfsInstance, service, subService);
   }

   public int getFailedServiceCount(String kthfsInstance, String service, boolean subService) {
      return serviceEJB.getFailedServicesCount(kthfsInstance, service, subService);
   }
   
   public Long getNdbCount() {
      return serviceEJB.findServiceCount(kthfsInstance, serviceGroup, "ndb");
   }

   public int getStoppedServiceCount(String kthfsInstance, String service, boolean subService) {
      return serviceEJB.getStoppedServicesCount(kthfsInstance, service, subService);
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

   public boolean hasWebUI() {


      Service s = serviceEJB.findServices(hostname, kthfsInstance, serviceGroup, service);
      if (s.getWebPort() == null) {
         return false;
      }
      return true;
   }

   public String showStdoutLog(int n) {
      WebCommunication webComm = new WebCommunication(hostname, kthfsInstance, service);
      return webComm.getStdOut(n);
   }

   public String showStderrLog(int n) {
      WebCommunication webComm = new WebCommunication(hostname, kthfsInstance, service);
      return webComm.getStdErr(n);
   }

   public String showConfig() throws Exception {
      WebCommunication webComm = new WebCommunication(hostname, kthfsInstance, service);
      return webComm.getConfig();
   }

   public String getNotAvailable() {
      return NOT_AVAILABLE;
   }
   
   
      public boolean getShowNdbInfo() {
      if (service == null) {
         return false;
      }
      if (service.equalsIgnoreCase("mysqld")) {
         return true;
      }
      return false;
   }
}