package se.kth.kthfsdashboard.mysqlaccess;

import java.io.Serializable;
import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import se.kth.kthfsdashboard.struct.NodesTableItem;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class MySQLController implements Serializable {

   @ManagedProperty("#{param.hostname}")
   private String hostname;
   @ManagedProperty("#{param.service}")
   private String service;
   @ManagedProperty("#{param.servicegroup}")
   private String serviceGroup;
   @ManagedProperty("#{param.kthfsinstance}")
   private String kthfsInstance;

   public MySQLController() {
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
   
   public List<NodesTableItem> getInfo() throws Exception {
      
          MySQLAccess dao = new MySQLAccess();
         return dao.readDataBase();
   }




}
