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
    @ManagedProperty("#{param.kthfsinstance}")
    private String kthfsInstance;

    
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

    public void setKthfsInstance(String kthfsInstance) {
        this.kthfsInstance = kthfsInstance;
    }

    public String getKthfsInstance() {
        return kthfsInstance;
    }


    public List<Command> getRecentCommandsByInstance() {
        List<Command> commands = commandEJB.findRecentByInstance(kthfsInstance);
        return commands;
    }

    public List<Command> getRunningCommandsByInstance() {
        List<Command> commands = commandEJB.findRunningByInstance(kthfsInstance);
        return commands;
    }

}
