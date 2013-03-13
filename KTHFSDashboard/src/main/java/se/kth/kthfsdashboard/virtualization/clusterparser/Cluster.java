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
    private List<String> globalServices;
    private List<String> authorizePorts;
    private List<Integer> authorizeSpecificPorts;
    private String environment;
    private Provider provider;
    private List<NodeGroup> nodes;
    private List<ChefAttributes> chefAttributes;

    public List<String> getGlobalServices() {
        return globalServices;
    }

    public void setGlobalServices(List<String> globalServices) {
        this.globalServices = globalServices;
    }

    public List<String> getAuthorizePorts() {
        return authorizePorts;
    }

    public void setAuthorizePorts(List<String> authorizePorts) {
        this.authorizePorts = authorizePorts;
    }

    public List<Integer> getAuthorizeSpecificPorts() {
        return authorizeSpecificPorts;
    }

    public void setAuthorizeSpecificPorts(List<Integer> authorizeSpecificPorts) {
        this.authorizeSpecificPorts = authorizeSpecificPorts;
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
   
    public List<NodeGroup> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeGroup> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "Cluster{" + "name=" + name + ", environment=" + environment + ", provider=" + provider + ", instances=" + nodes + '}';
    }
    
    
    
}
