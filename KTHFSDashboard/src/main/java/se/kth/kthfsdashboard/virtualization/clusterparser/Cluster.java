/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;

import java.util.List;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
public class Cluster {
    private String name;
    private List<String> services;
    private String environment;
    private Provider provider;
    private List<Instance> instances;
    private List<ChefAttributes> chefAttributes;

    public List<String> getServices() {
        return services;
    }

    public void setServices(List<String> services) {
        this.services = services;
    }

    

    
    public List<ChefAttributes> getChefAttributes() {
        return chefAttributes;
    }

    public void setChefAttributes(List<ChefAttributes> chefAttributes) {
        this.chefAttributes = chefAttributes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }
    
    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    public List<Instance> getInstances() {
        return instances;
    }

    public void setInstances(List<Instance> instances) {
        this.instances = instances;
    }

    @Override
    public String toString() {
        return "Cluster{" + "name=" + name + ", environment=" + environment + ", provider=" + provider + ", instances=" + instances + '}';
    }
    
    
    
}
