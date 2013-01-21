package se.kth.kthfsdashboard.mysqlaccess;

import java.io.Serializable;
import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import se.kth.kthfsdashboard.struct.NodesTableItem;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@ViewScoped
public class MySQLController implements Serializable {

   private boolean flag;   
   private List<NodesTableItem> info;   
   
   public MySQLController() {
   }

   public List<NodesTableItem> getInfo() throws Exception {
//      System.err.println("Getting...");
      
      info = loadItems();
      return info;
   }

   public boolean getFlag() {
      return flag;
   }

   public void setFlag(boolean flag) {
      this.flag = flag;
   }

   public void load(ActionEvent event) {

      System.err.println("Loading...");
      if (info != null) {
         return;
      }
      info = loadItems();
      setFlag(true);
   }
   
   
   private List<NodesTableItem> loadItems() {
      MySQLAccess dao = new MySQLAccess();
      try {
         return dao.readDataBase();
      } catch (Exception e) {
         return null;
      }
   }
}
