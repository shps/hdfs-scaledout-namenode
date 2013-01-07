/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.struct;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class KthfsInstanceInfo {

   private String name;
   private String status;
   private String health;
   private String type;
   private Map roleCounts;

   public KthfsInstanceInfo(String name, String type, String status, String health) {
      roleCounts = new HashMap<String, Integer>();
      this.name = name;
      this.type = type;
      this.status = status;
      this.health = health;

   }

   public String getName() {
      return name;
   }

   public String getType() {
      return type;
   }

   public String getStatus() {
      return status;
   }

   public String getHealth() {
      return health;
   }

   public Map getRoleCounts() {
      return roleCounts;
   }

   public void putToRoleCounts(String service, Integer count) {
      this.roleCounts.put(service, count);
   }
}