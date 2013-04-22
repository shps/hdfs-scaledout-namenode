package se.kth.kthfsdashboard.command;

import java.util.List;
import javax.ejb.EJB;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
//@ViewScoped
public class CommandController {

    @EJB
    private CommandEJB commandEJB;
    @ManagedProperty("#{param.hostname}")
    private String hostname;
    @ManagedProperty("#{param.service}")
    private String service;
    @ManagedProperty("#{param.servicegroup}")
    private String serviceGroup;
    @ManagedProperty("#{param.cluster}")
    private String cluster;

    
//    private HashMap<String, InstanceInfo> commands = new HashMap<String, InstanceInfo>();

    public CommandController() {


    }
    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getServiceGroup() {
        return serviceGroup;
    }

    public void setServiceGroup(String serviceGroup) {
        this.serviceGroup = serviceGroup;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getCluster() {
        return cluster;
    }


    public List<Command> getRecentCommandsByInstance() {
        List<Command> commands = commandEJB.findRecentByInstance(cluster);
        return commands;
    }

    public List<Command> getRunningCommandsByInstance() {
        List<Command> commands = commandEJB.findRunningByInstance(cluster);
        return commands;
    }

    public List<Command> getRecentCommandsByInstanceGroup() {
        List<Command> commands = commandEJB.findRecentByInstanceGroup(cluster, serviceGroup);
        return commands;
    }

    public List<Command> getRunningCommandsByInstanceGroup() {
        List<Command> commands = commandEJB.findRunningByInstanceGroup(cluster, serviceGroup);
        return commands;
    }    
}
