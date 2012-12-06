package se.kth.kthfsdashboard.command;

import se.kth.kthfsdashboard.service.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import se.kth.kthfsdashboard.host.Host;
import se.kth.kthfsdashboard.host.HostEJB;
import se.kth.kthfsdashboard.struct.InstanceFullInfo;
import se.kth.kthfsdashboard.struct.InstanceInfo;
import se.kth.kthfsdashboard.struct.KthfsInstanceInfo;
import se.kth.kthfsdashboard.struct.ServiceRoleInfo;
import se.kth.kthfsdashboard.util.Formatter;

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
