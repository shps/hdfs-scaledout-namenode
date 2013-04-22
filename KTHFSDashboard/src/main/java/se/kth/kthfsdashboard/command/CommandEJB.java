package se.kth.kthfsdashboard.command;

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
public class CommandEJB {

    @PersistenceContext(unitName = "kthfsPU")
    private EntityManager em;

    public CommandEJB() {
    }

    public List<Command> findAll() {

        TypedQuery<Command> query = em.createNamedQuery("Commands.findAll", Command.class);
        return query.getResultList();
    }
    
    public List<Command> findRecentByInstance(String instance) {

        TypedQuery<Command> query = 
                em.createNamedQuery("Commands.findRecentByInstance", Command.class)
                .setParameter("instance", instance)                
                .setParameter("status", Command.commandStatus.Running);;
        return query.getResultList();
    }

    public List<Command> findRunningByInstance(String instance) {

        TypedQuery<Command> query = 
                em.createNamedQuery("Commands.findRunningByInstance", Command.class)
                .setParameter("instance", instance)
                .setParameter("status", Command.commandStatus.Running);
        return query.getResultList();
    }
    
    public List<Command> findRecentByInstanceGroup(String instance, String group) {

        TypedQuery<Command> query = 
                em.createNamedQuery("Commands.findRecentByInstance-Group", Command.class)
                .setParameter("instance", instance).setParameter("group", group)                
                .setParameter("status", Command.commandStatus.Running);
        return query.getResultList();
    }

    public List<Command> findRunningByInstanceGroup(String instance, String group) {

        TypedQuery<Command> query = 
                em.createNamedQuery("Commands.findRunningByInstance-Group", Command.class)
                .setParameter("instance", instance).setParameter("group", group)
                .setParameter("status", Command.commandStatus.Running);
        return query.getResultList();
    }    

    public void persistCommand(Command command) {
        em.persist(command);
    }

    public void updateCommand(Command command) {
        em.merge(command);
    }


}
