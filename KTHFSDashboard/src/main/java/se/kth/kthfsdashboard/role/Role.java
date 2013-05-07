package se.kth.kthfsdashboard.role;

import java.io.Serializable;
import java.text.DecimalFormat;
import javax.persistence.*;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Entity
@Table(name = "Roles")
@NamedQueries({
   @NamedQuery(name = "Role.findClusters", query = "SELECT DISTINCT r.cluster FROM Role r"),
   
//   @NamedQuery(name = "Role.findServiceClass", query = "SELECT DISTINCT r.serviceClass FROM Role r WHERE r.cluster = :cluster"),   
   @NamedQuery(name = "Role.findServiceGroups", query = "SELECT DISTINCT r.serviceGroup FROM Role r WHERE r.cluster = :cluster"),
   
   @NamedQuery(name = "Role.find", query = "SELECT r FROM Role r WHERE r.hostname = :hostname AND r.cluster = :cluster AND r.serviceGroup = :serviceGroup AND r.role = :role"),

   @NamedQuery(name = "Role.findBy.Cluster", query = "SELECT r FROM Role r WHERE r.cluster = :cluster"),  
   @NamedQuery(name = "Role.findBy.Cluster.Role.Hostname", query = "SELECT r FROM Role r WHERE r.hostname = :hostname AND r.cluster = :cluster AND r.role = :role"),
   @NamedQuery(name = "Role.findBy-Cluster-Group", query = "SELECT r FROM Role r WHERE r.cluster = :cluster AND r.serviceGroup = :group"),
   @NamedQuery(name = "Role.findBy-Cluster-Group-Role", query = "SELECT r FROM Role r WHERE r.cluster = :cluster AND r.serviceGroup = :group AND r.role = :role"),
   @NamedQuery(name = "Role.findBy-Cluster-Group-Role-Status", query = "SELECT r FROM Role r WHERE r.cluster = :cluster AND r.serviceGroup = :group AND r.role = :role AND r.status = :status"),

// need this?   
   @NamedQuery(name = "Role.findHostnameBy-Cluster-Group-Role", query = "SELECT r.hostname FROM Role r WHERE r.cluster = :cluster AND r.serviceGroup = :group AND r.role = :role ORDER BY r.hostname"),

   @NamedQuery(name = "Role.Count", query="SELECT COUNT(r) FROM Role r WHERE r.cluster = :cluster AND r.serviceGroup = :serviceGroup AND r.role = :role")
})
public class Role implements Serializable {

   public enum Status {
      Started, Stopped, Failed, None, All
   }

   public enum Health {
      Good, Bad
   }

   public enum RoleType {
      namenode, datanode, 
      mgmserver, mysqld, ndb,
      resourcemanager, nodemanager,
   }    
   
   @Id
   @GeneratedValue(strategy = GenerationType.SEQUENCE)
   private Long id;
   @Column(name = "host_name", nullable = false, length = 128)
   private String hostname;
//   @Column(nullable = false)
//   private ServiceClass serviceClass;
   @Column(nullable = false, length = 48)
   private String serviceGroup;
   @Column(nullable = false, length = 48)
   private String role;
   @Column(nullable = false, length = 48)
   private String cluster;
   private long uptime;
   @Column(nullable = false)
   private Status status;
   private int pid;
   private Integer webPort;

   public Role() {
   }

   public Role(String hostname, String cluster, String serviceGroup, String role, Integer webPort, Role.Status status) {
      this.hostname = hostname;
      this.cluster = cluster;
//      this.serviceClass = serviceClass;
      this.serviceGroup = serviceGroup;
      this.role = role;
      this.status = status;
      this.webPort = webPort;
   }

   public Role(String hostname, String cluster, String serviceGroup, String role, Integer webPort) {
      this.hostname = hostname;
      this.cluster = cluster;
//      this.serviceClass = serviceClass;
      this.serviceGroup = serviceGroup;
      this.role = role;
      this.webPort = webPort;
   }

   public Role(String hostname, String cluster, String serviceGroup, String role) {
      this.hostname = hostname;
      this.cluster = cluster;
//      this.serviceClass = serviceClass;
      this.serviceGroup = serviceGroup;
      this.role = role;
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

//   public ServiceClass getServiceClass() {
//      return serviceClass;
//   }
//
//   public void setServiceClass(ServiceClass serviceClass) {
//      this.serviceClass = serviceClass;
//   }

   public String getServiceGroup() {
      return serviceGroup;
   }

   public void setServiceGroup(String serviceGroup) {
      this.serviceGroup = serviceGroup;
   }

   public String getRole() {
      return role;
   }

   public void setRole(String role) {
      this.role = role;
   }

   public String getCluster() {
      return cluster;
   }

   public void setCluster(String cluster) {
      this.cluster = cluster;
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
}