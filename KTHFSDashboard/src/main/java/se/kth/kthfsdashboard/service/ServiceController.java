package se.kth.kthfsdashboard.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import se.kth.kthfsdashboard.host.Host;
import se.kth.kthfsdashboard.host.HostEJB;
import se.kth.kthfsdashboard.struct.InstanceFullInfo;
import se.kth.kthfsdashboard.struct.InstanceInfo;
import se.kth.kthfsdashboard.struct.KthfsInstanceInfo;
import se.kth.kthfsdashboard.struct.ServiceRoleInfo;
import se.kth.kthfsdashboard.util.Formatter;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
//@ViewScoped
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

    public ServiceController() {

        kthfsInstances.put("hdfs1", new KthfsInstanceInfo("hdfs1", "HDFS", "started?", "?"));
//        kthfsInstances.put("hdfs2", new KthfsInstanceInfo("hdfs2", "HDFS", "started?", "?"));
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
        allKthfsInstances.addAll(kthfsInstances.values());
        return allKthfsInstances;
    }

    public String requestParams() {
        FacesContext context = FacesContext.getCurrentInstance();
        HttpServletRequest request = (HttpServletRequest) context.getExternalContext().getRequest();

        Principal principal = request.getUserPrincipal();
//           log.info("Authenticated user: " + principal.getName());  

        return request.getAuthType().toString() + " - "
                + principal.getName();

    }

    public List<InstanceInfo> getInstances() {
        List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
        List<Service> services = serviceEJB.findAllInstances();
        for (Service s : services) {
            instances.add(new InstanceInfo(s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
        }
        return instances;
    }

    public List<InstanceInfo> getInstance() {
        List<InstanceInfo> instances = new ArrayList<InstanceInfo>();
        List<Service> services = serviceEJB.findInstances(kthfsInstance, hostname, service);
        for (Service s : services) {
            instances.add(new InstanceInfo(s.getServiceGroup(), s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString()));
        }
        return instances;
    }

    public List<InstanceFullInfo> getInstanceFullInfo() {
        List<InstanceFullInfo> instances = new ArrayList<InstanceFullInfo>();
        List<Service> services = serviceEJB.findInstances(kthfsInstance, hostname, service);
        for (Service s : services) {
            InstanceFullInfo i = new InstanceFullInfo(s.getService(), s.getHostname(), "?", s.getStatus(), s.getHealth().toString());
            i.setPid(s.getPid());
            i.setUptime(Formatter.time(s.getUptime() * 1000));
            instances.add(i);
        }
        return instances;
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
        return "services-instances-status?faces-redirect=true&hostname=" + hostname + "&kthfsinstance=" + kthfsInstance + "&service=" + service;
    }

    public String gotoServiceStatus() {
        return "services-status?faces-redirect=true&kthfsinstance=" + kthfsInstance;
    }

    public String gotoServiceCommands() {
        return "services-commands?faces-redirect=true&kthfsinstance=" + kthfsInstance;
    }

    public String gotoServiceInstanceStatus() {
        return "services-instances-status?faces-redirect=true&kthfsinstance=" + kthfsInstance + "&hostname=" + hostname + "&service=" + service;
    }

    public String gotoServiceInstanceSubservices() {
        String url = "services-instances-subservices?faces-redirect=true&kthfsinstance=" + kthfsInstance + "&hostname=" + hostname;
        if (service != null) {
            url += "&service=" + service;
        }
        if (serviceGroup != null) {
            url += "&servicegroup=" + serviceGroup;
        }
        return url;
    }

    public String gotoServiceInstanceSubservicesInstance() {
        return "services-instances-subservices-instance?faces-redirect=true&kthfsinstance=" + kthfsInstance + "&hostname=" + hostname + "&service=" + service + "&servicegroup=" + serviceGroup;
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
        String instance = "hdfs1";
        int started, stopped, failed, good, bad;
        if (kthfsInstance.equalsIgnoreCase(instance)) {

            List<ServiceRoleInfo> roles = new ArrayList<ServiceRoleInfo>();
            roles.add(new ServiceRoleInfo("MySQL Cluster", "mysqlcluster"));
//            roles.add(new ServiceRoleInfo("MySQL Cluster NDBD (ndb)", "ndb"));
//            roles.add(new ServiceRoleInfo("MySQL Server (mysqld)", "mysqld"));
//            roles.add(new ServiceRoleInfo("MGM Server (mgmserver)", "mgmserver"));
            roles.add(new ServiceRoleInfo("NameNode", "namenode"));
            roles.add(new ServiceRoleInfo("DataNode", "datanode"));

            for (ServiceRoleInfo role : roles) {
                started = getStartedServiceCount(instance, role.getShortName(), false);
                stopped = getStoppedServiceCount(instance, role.getShortName(), false);
                failed = getFailedServiceCount(instance, role.getShortName(), false);
                good = started + stopped;
                bad = failed;
                role.setStatusStarted(started + " Started");
                role.setStatusStopped((stopped + failed) + " Stopped");
                role.setHealth(String.format("%d Good, %d Bad", good, bad));
                serviceRoles.add(role);
            }

        }

        return serviceRoles;
    }

    public List<ServiceRoleInfo> getMysqlClusterSuberviceRoles() {

        List<ServiceRoleInfo> serviceRoles = new ArrayList<ServiceRoleInfo>();
        String instance = "hdfs1";
        int started, stopped, failed, good, bad;
        if (kthfsInstance.equalsIgnoreCase(instance)) {

            List<ServiceRoleInfo> roles = new ArrayList<ServiceRoleInfo>();
            roles.add(new ServiceRoleInfo("MySQL Cluster NDBD (ndb)", "ndb"));
            roles.add(new ServiceRoleInfo("MySQL Server (mysqld)", "mysqld"));
            roles.add(new ServiceRoleInfo("MGM Server (mgmserver)", "mgmserver"));

            for (ServiceRoleInfo role : roles) {
                started = getStartedServiceCount(instance, role.getShortName(), true);
                stopped = getStoppedServiceCount(instance, role.getShortName(), true);
                failed = getFailedServiceCount(instance, role.getShortName(), true);
                good = started + stopped;
                bad = failed;
                role.setStatusStarted(started + " Started");
                role.setStatusStopped((stopped + failed) + " Stopped");
                role.setHealth(String.format("%d Good, %d Bad", good, bad));
                serviceRoles.add(role);
            }

        }

        return serviceRoles;
    }
    public boolean show;

    public boolean getShow() {
        return true;
    }

    public void doShow() {
//        this.show = false;
    }

//    public int getServiceCount(String role) {
//        return serviceEJB.findRoleCount(role);
//    }
    public int getStartedServiceCount(String kthfsInstance, String service, boolean subService) {
        return serviceEJB.getStartedServicesCount(kthfsInstance, service, subService);
    }

    public int getFailedServiceCount(String kthfsInstance, String service, boolean subService) {
        return serviceEJB.getFailedServicesCount(kthfsInstance, service, subService);
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

    public String showStdoutLog(int n) {

        Client client = Client.create();
        String log = NOT_AVAILABLE;
//        TODO: does not work with hostname!

        try {
            Host h = hostEJB.findHostByName(hostname);
            String url = "http://" + h.getIp() + ":8090/log/" + kthfsInstance + "/" + service + "/stdout/" + n;
            WebResource resource = client.resource(url);
            ClientResponse response = resource.get(ClientResponse.class);
            if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
                log = response.getEntity(String.class);
            }
        } catch (Exception e) {
        }
        log = log.replaceAll("\n", "<br>");
        return log;
    }

    public String showStderrLog(int n) {

        Client client = Client.create();
        String log = NOT_AVAILABLE;
//        TODO: does not work with hostname!
        try {
            Host h = hostEJB.findHostByName(hostname);
            String url = "http://" + h.getIp() + ":8090/log/" + kthfsInstance + "/" + service + "/stderr/" + n;
            WebResource resource = client.resource(url);
            ClientResponse response = resource.get(ClientResponse.class);
            if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
                log = response.getEntity(String.class);
            }
        } catch (Exception e) {
            System.err.println("Stderr Exception: " + e.toString());
        }
        log = log.replaceAll("\n", "<br>");
        return log;
    }

    public String showConfig() {

        Client client = Client.create();
        String conf = NOT_AVAILABLE;
        Host h = hostEJB.findHostByName(hostname);
        String url = "http://" + h.getIp() + ":8090/config/" + kthfsInstance + "/" + service;
        WebResource resource = client.resource(url);
        try {
            ClientResponse response = resource.get(ClientResponse.class);

            if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
                conf = response.getEntity(String.class);
            } 
        } catch (Exception e) {
            System.err.println(e.toString());
        }

        return conf;
    }
    
    
    public String getNotAvailable(){
        return NOT_AVAILABLE;
    }
}
