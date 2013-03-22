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
   
   private String longName;
   private String shortName;
   private int logLines;

   public SettingsController() {
      
      shortName = "JHadoop Dashboard";
      longName = "Jumbo Hadoop Dashboard";      
      logLines = 2;
   }

   public String getLongName() {
      return longName;
   }

   public String getShortName() {
      return shortName;
   }

   public int getLogLines() {
      return logLines;
   }

   
   

}