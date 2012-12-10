package se.kth.kthfsdashboard.command;

import se.kth.kthfsdashboard.service.*;
import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.Parameter;
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

    public void persistCommand(Command command) {
        em.persist(command);
    }

    public void updateCommand(Command command) {
        em.merge(command);
    }


}
