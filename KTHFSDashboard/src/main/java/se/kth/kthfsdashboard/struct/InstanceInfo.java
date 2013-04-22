package se.kth.kthfsdashboard.struct;

import java.io.Serializable;
import se.kth.kthfsdashboard.service.Service;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class InstanceInfo implements Serializable {

    private String name;
    private String host;
    private String cluster;
    private String service;
    private String serviceGroup;
    private String rack;
    private Service.Status status;
    private String health;

    public InstanceInfo(String cluster, String serviceGroup, String service, String host, String rack, Service.Status status, String health) {

        this.name = service + " (" + host + ")";
        this.host = host;
        this.cluster = cluster;
        this.serviceGroup = serviceGroup;
        this.service = service;
        this.rack = rack;
        this.status = status;
        this.health = health;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getRack() {
        return rack;
    }

    public Service.Status getStatus() {
        return status;
    }

    public String getHealth() {
        return health;
    }
    
    public String getService() {
        return service;
    }
    
    public String getServiceGroup(){
       return serviceGroup;
    }

   public String getCluster() {
      return cluster;
   }

   public void setCluster(String cluster) {
      this.cluster = cluster;
   }
            
}
