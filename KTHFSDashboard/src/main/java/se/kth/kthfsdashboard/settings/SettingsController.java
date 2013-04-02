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
   private int logLines;

   public SettingsController() {
      name = "Jumbo Hadoop Dashboard";
      logLines = 2;
   }

   public String getName() {
      return name;
   }
   

}