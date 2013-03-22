package se.kth.kthfsdashboard.settings;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@SessionScoped
public class SettingsController {
   
   private String name;


   public SettingsController() {
      
      name = "Jumbo Hadoop Dashboard";

   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }
   
   

}