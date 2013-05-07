package se.kth.kthfsdashboard.role;

import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Stateless
public class RoleEJB {

   @PersistenceContext(unitName = "kthfsPU")
   private EntityManager em;

   public RoleEJB() {
   }

   public List<String> findClusters() {
      TypedQuery<String> query = em.createNamedQuery("Role.findClusters", String.class);
      return query.getResultList();
   }

   public List<String> findServiceGroups(String cluster) {
      TypedQuery<String> query = em.createNamedQuery("Role.findServiceGroups", String.class)
              .setParameter("cluster", cluster);
      return query.getResultList();
   }

   public Role find(String hostname, String cluster, String serviceGroup, String role) {
      TypedQuery<Role> query = em.createNamedQuery("Role.find", Role.class)
              .setParameter("hostname", hostname).setParameter("cluster", cluster)
              .setParameter("serviceGroup", serviceGroup).setParameter("role", role);
      return query.getSingleResult();
   }

   public List<Role> findRoles(String cluster) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy.Cluster", Role.class)
              .setParameter("cluster", cluster);
      return query.getResultList();
   }

   public List<Role> findRoles(String cluster, String group) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy-Cluster-Group", Role.class)
              .setParameter("cluster", cluster).setParameter("group", group);
      return query.getResultList();
   }

   public List<Role> findRoles(String cluster, String group, String role) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy-Cluster-Group-Role", Role.class)
              .setParameter("cluster", cluster).setParameter("group", group).setParameter("role", role);
      return query.getResultList();
   }

   public List<Role> findRoles(String cluster, String group, String role, Role.Status status) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy-Cluster-Group-Role-Status", Role.class)
              .setParameter("cluster", cluster).setParameter("group", group).setParameter("role", role)
              .setParameter("status", status);
      return query.getResultList();
   }

   public List<Role> findByHostnameClusterRole(String host, String cluster, String role) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy.Cluster.Role.Hostname", Role.class)
              .setParameter("hostname", host).setParameter("role", role).setParameter("cluster", cluster);
      return query.getResultList();
   }

   public List<String> findHostname(String cluster, String group, String role) {
      TypedQuery<String> query = em.createNamedQuery("Role.findHostnameBy-Cluster-Group-Role", String.class)
              .setParameter("cluster", cluster).setParameter("group", group).setParameter("role", role);
      return query.getResultList();
   }

   public Long count(String cluster, String serviceGroup, String role) {
      TypedQuery<Long> query = em.createNamedQuery("Role.Count", Long.class)
              .setParameter("cluster", cluster)
              .setParameter("serviceGroup", serviceGroup)
              .setParameter("role", role);
      return query.getSingleResult();
   }

   public int countStatus(String cluster, String group, String role, Role.Status status) {
      TypedQuery<Role> query;
      query = em.createNamedQuery("Role.findBy-Cluster-Group-Role-Status", Role.class)
              .setParameter("cluster", cluster).setParameter("group", group).setParameter("role", role)
              .setParameter("status", status);
      return query.getResultList().size();
   }

   public void persist(Role role) {
      em.persist(role);
   }

   public void store(Role role) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy.Cluster.Role.Hostname", Role.class)
              .setParameter("hostname", role.getHostname()).setParameter("cluster", role.getCluster())
              .setParameter("role", role.getRole());
      List<Role> s = query.getResultList();

      if (s.size() > 0) {
         role.setId(s.get(0).getId());
         em.merge(role);
      } else {
         em.persist(role);
      }
   }

   // ???
   public void delete(Role role) {
      TypedQuery<Role> query = em.createNamedQuery("Role.findBy.Cluster.Role.Hostname", Role.class)
              .setParameter("hostname", role.getHostname()).setParameter("cluster", role.getCluster())
              .setParameter("role", role.getRole());
      Role s = query.getSingleResult();
      em.remove(s);
   }
}