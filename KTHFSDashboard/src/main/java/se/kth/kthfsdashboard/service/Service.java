package se.kth.kthfsdashboard.service;

import java.io.Serializable;
import java.text.DecimalFormat;
import javax.persistence.*;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Entity
//@IdClass(ServiceId.class)
@Table(name = "Services")
@NamedQueries({
   
//    @NamedQuery(name = "findInstances", query = "SELECT DISTINCT s.instance FROM Service s"),
    
    @NamedQuery(name = "findAllServices", query = "SELECT s FROM Service s WHERE s.serviceGroup = s.service"),
    @NamedQuery(name = "findAllSubservices", query = "SELECT s FROM Service s WHERE NOT s.serviceGroup = s.service"),
    @NamedQuery(name = "findService", query = "SELECT s FROM Service s WHERE s.hostname = :hostname AND s.instance = :instance AND s.serviceGroup = :serviceGroup AND s.service = :service"),

    
    @NamedQuery(name = "findServiceBy-Hostname", query = "SELECT s FROM Service s WHERE s.hostname = :hostname"),
    @NamedQuery(name = "findServiceBy-Instance", query = "SELECT s FROM Service s WHERE s.serviceGroup = s.service AND s.instance = :instance"),
    @NamedQuery(name = "findServiceBy-Instance-ServiceGroup", query = "SELECT s FROM Service s WHERE s.serviceGroup = s.service AND s.serviceGroup = :serviceGroup AND s.instance = :instance"),
    
    @NamedQuery(name = "findSubserviceBy-Instance", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.instance = :instance"),    
    @NamedQuery(name = "findSubserviceBy-Instance-ServiceGroup", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.instance = :instance AND s.serviceGroup = :serviceGroup"),
    @NamedQuery(name = "findSubserviceBy-Instance-ServiceGroup-Service", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.instance = :instance AND s.serviceGroup = :serviceGroup AND s.service = :service"),
    
    @NamedQuery(name = "findServiceBy-Instance-Service", query = "SELECT s FROM Service s WHERE (s.serviceGroup = s.service) AND s.instance = :instance AND s.service = :service"),
    @NamedQuery(name = "findServiceBy-Instance-Service-Status", query = "SELECT s FROM Service s WHERE (s.serviceGroup = s.service) AND s.instance = :instance AND s.service = :service AND s.status = :status"),
    @NamedQuery(name = "findServiceBy-Instance-Service-Hostname", query = "SELECT s FROM Service s WHERE s.hostname = :hostname AND s.instance = :instance AND s.service = :service"),
    
    @NamedQuery(name = "findSubserviceBy-Instance-Service", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.instance = :instance AND s.service = :service"),
    @NamedQuery(name = "findSubserviceBy-Instance-Service-Status", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.instance = :instance AND s.service = :service AND s.status = :status"),
    @NamedQuery(name = "findSubserviceBy-Instance-Service-Hostname", query = "SELECT s FROM Service s WHERE (NOT s.serviceGroup = s.service) AND s.hostname = :hostname AND s.instance = :instance AND s.service = :service"),

    
    
    
    @NamedQuery(name = "deleteServicesByHostname", query = "DELETE FROM Service s WHERE s.hostname = :hostname"),
    @NamedQuery(name = "findService-Filter-Instance-Service-Status", query = "SELECT s FROM Service s WHERE (:instance is null OR s.instance = :instance) AND (:service is null OR s.service = :service) AND (s.status = :status)"),
    @NamedQuery(name = "findService-Filter-Instance-Service", query = "SELECT s FROM Service s WHERE (:instance is null OR  s.instance = :instance) AND (:service is null OR s.service = :service)")
})
public class Service implements Serializable {


    public enum Status {

        Started, Stopped, Failed, None, All
    }

    public enum Health {

        Good, Bad
    }
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Long id;
    @Column(name = "host_name", nullable = false, length = 128)
    private String hostname;
    @Column(nullable = false, length = 48)
    private String serviceGroup;
    @Column(nullable = false, length = 48)
    private String service;
    @Column(nullable = false, length = 48)
    private String instance;
    private long uptime;
//    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;
    private int pid;
    @Column(name = "web_port")
    private Integer webPort;

    public Service() {
    }

    public Service(String hostname, String instance, String serviceGroup, String service, Integer webPort, Service.Status status) {
        this.hostname = hostname;
        this.instance = instance;
        this.serviceGroup = serviceGroup;
        this.service = service;
        this.status = status;
        this.webPort = webPort;
    }

    public Service(String hostname, String instance, String serviceGroup, String service, Integer webPort) {
        this.hostname = hostname;
        this.instance = instance;
        this.serviceGroup = serviceGroup;
        this.service = service;
        this.webPort = webPort;
    }

    public Service(String hostname, String instance, String serviceGroup, String service) {
        this.hostname = hostname;
        this.instance = instance;
        this.serviceGroup = serviceGroup;
        this.service = service;
    }
    
    public static Status getServiceStatus(String status) {
        try {
            return Status.valueOf(status);
        } catch (Exception ex) {
            return Status.None;
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getServiceGroup() {
        return serviceGroup;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }
    
    public Integer getWebPort() {
      return webPort;
    }

   public void setWebPort(Integer webPort) {
      this.webPort = webPort;
   }

   
    public String getUptimeInSeconds() {

        DecimalFormat df = new DecimalFormat("#,###,##0.0");
        return df.format(uptime / 1000);
    }

    public Health getHealth() {

        if (status == Status.Failed) {
            return Health.Bad;
        }
        return Health.Good;
    }
    
    
//
//    public String getDiskInfo() {
//        Formatter f = new Formatter();
//        return f.storage(diskUsed) + " / " + f.storage(diskCapacity);
//    }
//    public String getUptimeInSeconds() {
//
////        return String.format("%1.1f", ((double) ((new Date()).getTime() - getLastHeartbeat())) / 1000);
//        DecimalFormat df = new DecimalFormat("#,###,##0.0");
//        return df.format(((double) ((new Date()).getTime() - getLastHeartbeat())) / 1000);
//    }
//    public String getMemoryInfo() {
//
//        String h = hostname;
//        CollectdTools ct = new CollectdTools();
//        Long free = ct.getLatestMemoryStatus(h, "free");
//        Long used = ct.getLatestMemoryStatus(h, "used");
//        Long cached = ct.getLatestMemoryStatus(h, "cached");
//        Long buffered = ct.getLatestMemoryStatus(h, "buffered");
//        MemoryInfo m = new MemoryInfo(used, cached, buffered, free);
//
////        String.format("%1$,.2f", doubleVal);
//        return String.format("%1.1f", m.getFreePercentage()) + "% used";
//    }
}