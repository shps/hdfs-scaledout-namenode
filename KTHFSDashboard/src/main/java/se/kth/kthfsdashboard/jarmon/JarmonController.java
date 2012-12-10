package se.kth.kthfsdashboard.jarmon;

import java.io.Serializable;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class JarmonController implements Serializable {

    @ManagedProperty("#{param.service}")
    private String service;
    @ManagedProperty("#{param.hostname}")
    private String hostname;

    public JarmonController() {
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public boolean getShowJarmonChart() {

        if (service == null) {
            return false;
        }

        if (service.equalsIgnoreCase("namenode") || 
                service.equalsIgnoreCase("datanode") || 
                service.equalsIgnoreCase("mysqld")) {
            return true;
        }
        return false;
    }

    public String getJarmonUrl() {

        if (service.equalsIgnoreCase("namenode")) {
            return "jarmon-" + service + ".xhtml?hostname=" + hostname + "&jmxparam=FSNamesystem";
        } else if (service.equalsIgnoreCase("datanode")) {
            return "jarmon-" + service + ".xhtml?hostname=" + hostname + "&jmxparam=DataNodeActivity";
        } else if (service.equalsIgnoreCase("mysqld")) {
            return "jarmon-" + service + ".xhtml?hostname=" + hostname;
        }
        return null;
    }

    public String gotoNamenodeActivity() {
        return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=NameNodeActivity";
    }

    public String gotoFSNamesystem() {
        return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=FSNamesystem";
    }
    
    public String gotoFSNamesystemState() {
        return "jarmon-namenode?faces-redirect=true&hostname=" + hostname + "&jmxparam=FSNamesystemState";
    }
    
    
    public String gotoDataNodeActivity() {
        return "jarmon-datanode?faces-redirect=true&hostname=" + hostname + "&jmxparam=DataNodeActivity";
    }
}
