package se.kth.kthfsdashboard.host;

import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.ejb.EJB;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import se.kth.kthfsdashboard.struct.DiskInfo;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
//@SessionScoped
@RequestScoped
public class HostController implements Serializable {

   @EJB
   private HostEJB hostEJB;

   @ManagedProperty("#{param.kthfsinstance}")
   private String kthfsInstance;
   @ManagedProperty("#{param.hostname}")
   private String hostname;
   @ManagedProperty("#{param.command}")
   private String command;
   @ManagedProperty("#{param.servicegroup}")
   private String serviceGroup;
   @ManagedProperty("#{param.service}")
   private String service;
   
   private Host host;
   private boolean currentHostAvailable;
   private long lastUpdate;
   private int memoryUsed; //percentage
   private int swapUsed; //percentage
   private String load;
   private String health;
   private List<DiskInfo> df;
   private int cpuCount;
   private HashMap<String, List<String>> commandsMap;

   public HostController() {

      commandsMap = new HashMap<String, List<String>>();
      commandsMap.put("all", Arrays.asList("install", "uninstall"));

   }

   public String gotoHost() {

      return "host?faces-redirect=true&hostname=" + hostname;
   }

   public void doSetRackId() {

      hostEJB.storeHostRackId(host);
   }

   public List<String> getCommands() {

      return commandsMap.get("all");
   }

   public void setCommand(String command) {
      this.command = command;
   }

   public String getCommand() {
      return command;
   }

   public void setService(String service) {
      this.service = service;
   }

   public String getService() {
      return service;
   }

   public void setHostname(String hostname) {
      this.hostname = hostname;
   }

   public void setServiceGroup(String serviceGroup) {
      this.serviceGroup = serviceGroup;
   }

   public String getServiceGroup() {
      return serviceGroup;
   }

   public String getHostname() {
      return hostname;
   }

   public void setKthfsInstance(String kthfsInstance) {
      this.kthfsInstance = kthfsInstance;
   }

   public String getKhfsInstance() {
      return kthfsInstance;
   }

   public List<Host> getHosts() {
      return hostEJB.findHosts();
   }

   public Host getHost() {
      host = hostEJB.findHostByName(hostname);
      return host;
   }

   public boolean isCurrentHostAvailable() {
      return currentHostAvailable;
   }

   public String getHealth() {
      return "Good!";
   }

   
   public void doCommand(ActionEvent actionEvent) throws NoSuchAlgorithmException, Exception {

      FacesContext.getCurrentInstance().addMessage(null, null);
      
   }

}