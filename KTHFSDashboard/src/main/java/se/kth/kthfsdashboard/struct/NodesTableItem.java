package se.kth.kthfsdashboard.struct;

import java.io.Serializable;
import se.kth.kthfsdashboard.util.Formatter;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */


public class NodesTableItem implements Serializable {

    private int nodeId;
    private String status;
    private long upTime;
    private int startPhase;
    private int configGeneration;

    public NodesTableItem(int nodeId, String status, long uptime, int startPhase, int configGeneration) {

        this.nodeId = nodeId;
        this.status = status;
        this.upTime = uptime;
        this.startPhase = startPhase;
        this.configGeneration = configGeneration;
    }

   public Integer getNodeId() {
      return nodeId;
   }

   public void setNodeId(Integer nodeId) {
      this.nodeId = nodeId;
   }

   public String getStatus() {
      return status;
   }

   public void setStatus(String status) {
      this.status = status;
   }

   public String getUpTime() {
      return Formatter.time(upTime);
   }

   public void setUpTime(Long upTime) {
      this.upTime = upTime;
   }

   public int getStartPhase() {
      return startPhase;
   }

   public void setStartPhase(int startPhase) {
      this.startPhase = startPhase;
   }

   public int getConfigGeneration() {
      return configGeneration;
   }

   public void setConfigGeneration(int configGeneration) {
      this.configGeneration = configGeneration;
   }

}
