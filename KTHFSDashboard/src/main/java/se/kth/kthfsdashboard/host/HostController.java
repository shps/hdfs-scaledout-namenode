package se.kth.kthfsdashboard.host;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.Serializable;
import java.security.Provider;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import javax.ws.rs.core.Response.Status.Family;
import org.codehaus.jettison.json.JSONObject;
import se.kth.kthfsdashboard.command.Command;
import se.kth.kthfsdashboard.command.CommandEJB;
import se.kth.kthfsdashboard.log.Log;
import se.kth.kthfsdashboard.log.LogEJB;
import se.kth.kthfsdashboard.service.Service;
import se.kth.kthfsdashboard.service.ServiceEJB;
import se.kth.kthfsdashboard.struct.DiskInfo;
import se.kth.kthfsdashboard.util.CollectdTools;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
//@SessionScoped
@RequestScoped
public class HostController implements Serializable {

   @EJB
   private LogEJB logEJB;
   @EJB
   private HostEJB hostEJB;
   @EJB
   private ServiceEJB serviceEJB;
   @EJB
   private CommandEJB commandEJB;
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
//    private int numberOfCpu; // EJB
   private int memoryUsed; //percent
   private int swapUsed; //percent   
   private String load;
   private String health;
   private List<DiskInfo> df;
   private int cpuCount;
   private CollectdTools collectdTools = new CollectdTools();
   private HashMap<String, List<String>> commandsMap;

   public HostController() {

      commandsMap = new HashMap<String, List<String>>();
      commandsMap.put("all", Arrays.asList("install", "uninstall"));

//        System.out.println("OK");
   }

   public String gotoHost() {
//        FacesContext context = FacesContext.getCurrentInstance();
//        Host h = context.getApplication().evaluateExpressionGet(context, "#{host}", Host.class);

      return "host?faces-redirect=true&hostname=" + hostname;
   }

   public void doSetRackId() {

      hostEJB.storeHostRackId(host);
   }

   public void doCommand(ActionEvent actionEvent) {

      //store command in database
      //  TODO: If the web application server craches, status will remain 'Running'.
      Command c = new Command(command, hostname, serviceGroup, service, kthfsInstance);
      commandEJB.persistCommand(c);

      FacesMessage message;
      Client client = Client.create();
      Host h = hostEJB.findHostByName(hostname);
      String url = "http://" + h.getIp() + ":8090/do/" + kthfsInstance + "/" + service + "/" + command;
      WebResource resource = client.resource(url);
      try {
         ClientResponse response = resource.get(ClientResponse.class);

         if (response.getClientResponseStatus().getFamily() == Family.SUCCESSFUL) {

            c.succeeded();
            commandEJB.updateCommand(c);

            String msg = "";
            if (command.equalsIgnoreCase("install")) {
               Service s = new Service(hostname, kthfsInstance, serviceGroup, service);
               s.setStatus(Service.serviceStatus.Stopped);
               serviceEJB.persistService(s);
               msg = command + ": " + response.getEntity(String.class);

            } else if (command.equalsIgnoreCase("uninstall")) {
               Service s = new Service(hostname, kthfsInstance, serviceGroup, service);
               serviceEJB.deleteService(s);
               msg = command + ": " + response.getEntity(String.class);

            } else if (command.equalsIgnoreCase("start")) {

               JSONObject json = new JSONObject(response.getEntity(String.class));
               msg = json.getString("msg");
               Service s = new Service(hostname, kthfsInstance, serviceGroup,service);
               s.setStatus(Service.serviceStatus.Started);
               serviceEJB.storeService(s);

            } else if (command.equalsIgnoreCase("stop")) {

               msg = command + ": " + response.getEntity(String.class);
               Service s = new Service(hostname, kthfsInstance, serviceGroup, service);
               s.setStatus(Service.serviceStatus.Stopped);
               serviceEJB.storeService(s);
            }
            message = new FacesMessage(FacesMessage.SEVERITY_INFO, "Success", msg);
         } else {

            c.failed();
            commandEJB.updateCommand(c);

            if (response.getStatus() == 400) {
               message = new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", command + ": " + response.getEntity(String.class));
            } else {
               message = new FacesMessage(FacesMessage.SEVERITY_FATAL, "Server Error", "");
            }
         }
      } catch (Exception e) {
         c.failed();
         commandEJB.updateCommand(c);
         message = new FacesMessage(FacesMessage.SEVERITY_FATAL, "Communication Error", e.toString());
      }
      FacesContext.getCurrentInstance().addMessage(null, message);
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

   public long getLastUpdate() {

      Long time = logEJB.findLatestLogTime(hostname).longValue();
      this.lastUpdate = (new Date()).getTime() / 1000 - time;
      return lastUpdate;
   }

//    public int getNumberOfCpu() {
//
//        this.numberOfCpu = logEJB.findNumberOfCpu(hostName).intValue();
//        return numberOfCpu;
//    }
   public int getMemoryUsed() {

      List<Log> logs = logEJB.findLatestLogForPlugin(hostname, "memory");

      double total = 0, used = 0, free, cached, buffered;
      DecimalFormat format = new DecimalFormat("#.##");

      for (Log l : logs) {

         String stringValue = l.getValues().substring(1, l.getValues().length() - 1);
         double doubleValue;
         try {
            doubleValue = format.parse(stringValue).doubleValue();
            total += doubleValue;
            if (l.getTypeInstance().equals("used")) {
               used = doubleValue;
            }
         } catch (Exception e) {
            System.err.println("Exception:" + e);
         }
      }

      this.memoryUsed = ((Long) (Math.round((used / total) * 100))).intValue();

      return memoryUsed;
   }

   public int getSwapUsed() {

      List<Log> logs = logEJB.findLatestLogForPlugin(hostname, "swap");

      double total = 0, used = 0;
      DecimalFormat format = new DecimalFormat("#.##");

      for (Log l : logs) {

         String stringValue = l.getValues().substring(1, l.getValues().length() - 1);
         double doubleValue;
         try {
            doubleValue = format.parse(stringValue).doubleValue();

            if (l.getType().equals("swap")) {
               total += doubleValue;
               if (l.getTypeInstance().equals("used")) {
                  used = doubleValue;
               }
            }
         } catch (Exception e) {
            System.err.println("Exception:" + e);
         }
      }

      this.swapUsed = ((Long) (Math.round((used / total) * 100))).intValue();
      return swapUsed;
   }

   public String getHealth() {
      return "Good!";
   }

   public String getLoad() {

      Log log = logEJB.findLatestLogForPluginAndType(hostname, "load", "load");

      String loads = "";
      for (String l : log.getValues().split("[\\[\\],]")) {
         if (!loads.isEmpty()) {
            loads += " ";
         }
         loads += l;
      }
      this.load = loads;

      return load;
   }

   public List<DiskInfo> getDf() throws ParseException {

      List<DiskInfo> diskInfoList = new ArrayList<DiskInfo>();
      List<Log> logs = logEJB.findLatestLog(hostname, "df", "df");
      for (Log log : logs) {
         diskInfoList.add(new DiskInfo(log.getTypeInstance(), log.getValues()));
      }
      return diskInfoList;
   }

   public String getInterfaces() {

      return collectdTools.typeInstances(hostname, "interface").toString();
   }
}
