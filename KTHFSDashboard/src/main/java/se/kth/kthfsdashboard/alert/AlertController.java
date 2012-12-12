package se.kth.kthfsdashboard.alert;

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
public class AlertController {

    @EJB
    private AlertEJB alertEJB;
    
    @ManagedProperty("#{param.hostname}")
    private String hostname;
    @ManagedProperty("#{param.service}")
    private String service;
    @ManagedProperty("#{param.servicegroup}")
    private String serviceGroup;
    @ManagedProperty("#{param.kthfsinstance}")
    private String kthfsInstance;


    public AlertController() {


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


    public List<Alert> getAlerts() {
        List<Alert> alert = alertEJB.findAll();
        return alert;
    }



}
