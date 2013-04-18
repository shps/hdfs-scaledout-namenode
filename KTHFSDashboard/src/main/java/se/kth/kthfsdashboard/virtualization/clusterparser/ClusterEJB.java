
package se.kth.kthfsdashboard.virtualization.clusterparser;

import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;


/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
@Stateless
public class ClusterEJB {
    
    @PersistenceContext(unitName="kthfsPU")
    private EntityManager em;
    
    public ClusterEJB(){
        
    }
    
    public void persistCluster(Cluster cluster){
        em.persist(cluster);
    }
    
    public void removeCluster(Cluster cluster){
        em.remove(em.merge(cluster));
    }
    
    
     public List<Cluster> findAll() {

        TypedQuery<Cluster> query = em.createNamedQuery("Clusters.findAll", Cluster.class);
        return query.getResultList();
    }
}
