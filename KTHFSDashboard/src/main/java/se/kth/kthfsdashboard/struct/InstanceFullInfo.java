package se.kth.kthfsdashboard.struct;

import java.io.Serializable;
import se.kth.kthfsdashboard.service.Service;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class InstanceFullInfo implements Serializable {

    private String name;
    private String host;
    private String service;
    private String rack;
    private Service.serviceStatus status;
    private String health;
    private int pid;
    private String uptime;

    public InstanceFullInfo(String service, String host, String rack, Service.serviceStatus status, String health) {

        this.name = service + " (" + host + ")";
        this.host = host;
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

    public Service.serviceStatus getStatus() {
        return status;
    }

    public String getHealth() {
        return health;
    }
    
    public String getService() {
        return service;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }
}
