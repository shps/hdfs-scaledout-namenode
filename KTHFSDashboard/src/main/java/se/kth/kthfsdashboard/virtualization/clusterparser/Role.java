/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;

import java.util.Map;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
public class Role {
    String name;
    Map<String,String> attributes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Role{" + "name=" + name + ", attributes=" + attributes + '}';
    }
    
    
}
