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
public class SecurityGroup {
    
    private String name;
    private List<Integer> portRange;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getPortRange() {
        return portRange;
    }

    public void setPortRange(List<Integer> portRange) {
        this.portRange = portRange;
    }

    @Override
    public String toString() {
        return "SecurityGroup{" + "name=" + name + ", portRange=" + portRange + '}';
    }
    
    
}
