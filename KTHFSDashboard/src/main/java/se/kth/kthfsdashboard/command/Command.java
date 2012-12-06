package se.kth.kthfsdashboard.command;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.persistence.*;
import se.kth.kthfsdashboard.util.Formatter;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Entity
@Table(name = "Commands")
@NamedQueries({
   @NamedQuery(name = "Commands.findAll", query = "SELECT c FROM Command c"),
   @NamedQuery(name = "Commands.findRecentByInstance", query = "SELECT c FROM Command c WHERE c.instance = :instance AND (NOT c.status = :status)  ORDER BY c.startTime DESC"),
   @NamedQuery(name = "Commands.findRunningByInstance", query = "SELECT c FROM Command c WHERE c.instance = :instance AND c.status = :status  ORDER BY c.startTime DESC")
})
public class Command implements Serializable {

   public enum commandStatus {

      Running, Succeeded, Failed
   }
   @Id
   @GeneratedValue(strategy = GenerationType.SEQUENCE)
   private Long id;
   @Column(name = "command", nullable = false, length = 256)
   private String command;
   @Column(name = "host_name", nullable = false, length = 128)
   private String hostname;
   @Column(nullable = false, length = 48)
   private String serviceGroup;
   @Column(nullable = false, length = 48)
   private String service;
   @Column(nullable = false, length = 48)
   private String instance;
   @Column(name = "start_time")
   @Temporal(javax.persistence.TemporalType.TIMESTAMP)
   private Date startTime;
   @Column(name = "end_time")
   @Temporal(javax.persistence.TemporalType.TIMESTAMP)
   private Date endTime;
   private commandStatus status;

   public Command() {
   }

   public Command(String command, String hostname, String serviceGroup, String service, String instance) {
      this.command = command;
      this.hostname = hostname;
      this.serviceGroup = serviceGroup;
      this.service = service;
      this.instance = instance;

      this.startTime = new Date();
      this.status = commandStatus.Running;
   }

   public Long getId() {
      return id;
   }

   public String getCommand() {
      return command;
   }

   public String getHostname() {
      return hostname;
   }

   public String getServiceGroup() {
      return serviceGroup;
   }

   public String getService() {
      return service;
   }

   public String getInstance() {
      return instance;
   }

   public Date getStartTime() {
      return startTime;
   }

   public String getStartTimeShort() {
      return Formatter.date(startTime);
   }

   public Date getEndTime() {
      return endTime;
   }

   public String getEndTimeShort() {
      return Formatter.date(endTime);
   }

   public commandStatus getStatus() {
      return status;
   }

   public void succeeded() {

      this.endTime = new Date();
      this.status = commandStatus.Succeeded;

   }

   public void failed() {
      this.endTime = new Date();
      this.status = commandStatus.Failed;
   }
}