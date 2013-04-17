package com.mycompany.clusterjtestapp;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.*;

import java.util.Properties;
import java.util.List;
import java.util.Random;

public class App {

    public static void main(String[] args) throws java.io.FileNotFoundException, java.io.IOException {
        // Load the properties from the clusterj.properties file

        File propsFile = new File("clusterj.properties");
        InputStream inStream = new FileInputStream(propsFile);
        Properties props = new Properties();
        props.load(inStream);

        //Used later to get userinput
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        // Create a session (connection to the database)
        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        // Create and initialise an Employee
        Employee newEmployee = session.newInstance(Employee.class);
        Random rand = new Random();
        
        rand.setSeed(System.currentTimeMillis());
        int id = rand.nextInt();
        newEmployee.setId(id);
        newEmployee.setFirst("John");
        newEmployee.setLast("Jones");
        newEmployee.setStarted("1 February 2009");
        newEmployee.setDepartment(666);

        // Write the Employee to the database
        session.persist(newEmployee);

        //Fetch the Employee from the database
        Employee theEmployee = session.find(Employee.class, id);

        if (theEmployee == null) {
            System.out.println("Could not find employee");
        } else {
            System.out.println("ID: " + theEmployee.getId() + "; Name: "
                    + theEmployee.getFirst() + " " + theEmployee.getLast());
            System.out.println("Location: " + theEmployee.getCity());
            System.out.println("Department: " + theEmployee.getDepartment());
            System.out.println("Started: " + theEmployee.getStarted());
            System.out.println("Left: " + theEmployee.getEnded());
        }

    }
}