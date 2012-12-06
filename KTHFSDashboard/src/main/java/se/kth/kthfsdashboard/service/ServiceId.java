package se.kth.kthfsdashboard.service;

import java.io.Serializable;


/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */

public class ServiceId  implements Serializable{

   private String instance;
   private String service;
   private String hostname;
   
   
   public ServiceId(){
       
   }
   
    @Override
    public boolean equals(Object o){
        if(o==null){
                return false;
        }

        if(o instanceof ServiceId){
                final ServiceId serviceId=(ServiceId)o;
                return serviceId.getHostname().equals(this.getHostname()) &&
                        serviceId.getInstance().equals(this.getInstance()) &&
                        serviceId.getService().equals(this.getService());
        }
        return false;
    }

    @Override
    public int hashCode(){
        return super.hashCode();
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
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

}