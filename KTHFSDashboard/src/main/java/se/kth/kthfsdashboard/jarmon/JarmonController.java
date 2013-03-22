package se.kth.kthfsdashboard.jarmon;

import java.io.Serializable;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import org.primefaces.context.RequestContext;
import org.primefaces.event.TabChangeEvent;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class JarmonController implements Serializable {

   @ManagedProperty("#{param.hostname}")
   private String hostname;
   @ManagedProperty("#{param.service}")
   private String service;
   @ManagedProperty("#{param.servicegroup}")
   private String serviceGroup;
   @ManagedProperty("#{param.kthfsinstance}")
   private String kthfsInstance;

   public JarmonController() {
   }

   public String getService() {
      return service;
   }

   public void setService(String service) {
      this.service = service;
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

   public void setKthfsInstance(String kthfsInstance) {
      this.kthfsInstance = kthfsInstance;
   }

   public String getKthfsInstance() {
      return kthfsInstance;
   }
   
   public boolean getShowJmxGraphs() {
      if (service == null) {
         return false;
      }
      if (service.equalsIgnoreCase("namenode")
              || service.equalsIgnoreCase("datanode")) {
         return true;
      }
      return false;
   }     

   public boolean getShowJarmonChartForJmx() {
      if (service == null) {
         return false;
      }
      if (service.equalsIgnoreCase("namenode")
              || service.equalsIgnoreCase("datanode")) {
         return true;
      }
      return false;
   }  

   public String getJarmonUrlForJmx() {

      if (service.equalsIgnoreCase("namenode")) {
         return "jarmon-" + service + ".xhtml?hostname=" + hostname + "&jmxparam=FSNamesystem";
      } else if (service.equalsIgnoreCase("datanode")) {
         return "jarmon-" + service + ".xhtml?hostname=" + hostname + "&jmxparam=DataNodeActivity";
      }
      return null;
   }

   public String getJarmonUrlForNdb() {

      if (service.equalsIgnoreCase("mysqld")) {
         return "jarmon-" + service + ".xhtml?hostname=" + hostname + "&kthfsinstance=" + kthfsInstance + "&servicegroup=" + serviceGroup + "&service=" + service;
      }
      return null;
   }

   public String gotoNamenodeActivity() {
      return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=NameNodeActivity";
   }

   public String gotoFSNamesystem() {
      return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=FSNamesystem";
   }

   public String gotoFSNamesystemState() {
      return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=FSNamesystemState";
   }

   public String gotoDataNodeActivity() {
      return "jarmon-datanode?faces-redirect=true&hostname=" + hostname + "&jmxparam=DataNodeActivity";
   }

   public void onTabChange(TabChangeEvent event) {
      String id = event.getTab().getId();
      RequestContext requestContext = RequestContext.getCurrentInstance();
      requestContext.execute("reload('" + id + "')");
   }
}
