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
public class NodeGroup {
    private String securityGroup;
    private int number;
    private List<String> roles;
    private List<String> authorizePorts;

    public String getSecurityGroup() {
        return securityGroup;
    }

    public void setSecurityGroup(String securityGroup) {
        this.securityGroup = securityGroup;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public List<String> getAuthorizePorts() {
        return authorizePorts;
    }

    public void setAuthorizePorts(List<String> authorizePorts) {
        this.authorizePorts = authorizePorts;
    }

    @Override
    public String toString() {
        return "NodeGroup{" + "securityGroup=" + securityGroup + ", number=" + number + ", roles=" + roles + ", authorizePorts=" + authorizePorts + '}';
    }
         
    
}
