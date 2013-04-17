/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.clusterjtestapp;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

@PersistenceCapable(table = "employee")
@Index(name = "idx_uhash")
public interface Employee {

    @PrimaryKey
    int getId();

    void setId(int id);

    String getFirst();

    void setFirst(String first);

    String getLast();

    void setLast(String last);

    @Column(name = "municipality")
    @Index(name = "idx_municipality")
    String getCity();

    void setCity(String city);

    String getStarted();

    void setStarted(String date);

    String getEnded();

    void setEnded(String date);

    Integer getDepartment();

    void setDepartment(Integer department);
}