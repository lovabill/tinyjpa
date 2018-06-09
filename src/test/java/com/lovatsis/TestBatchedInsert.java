package com.lovatsis;

import com.lovatsis.model.Person;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestBatchedInsert {

    private EntityManagerFactory emf = Persistence.createEntityManagerFactory("primary");

    @Test
    public void testPersons() {

        long total = 1000;

        Date startTime = new Date();
        persistNewObjectsInParallelBatchesWithMultipleTransactions(total);
        Date finishTime = new Date();

        EntityManager em = emf.createEntityManager();

        long count = em.createQuery("select count(p) from Person p", Long.class).getSingleResult();
        Assert.assertEquals(total, count);
        System.out.println("ms: " + (finishTime.getTime() - startTime.getTime()));

        em.close();
    }

    private void persistNewObjectsInParallelBatchesWithMultipleTransactions(long total) {
        int threads = 4; //Number of threads

        List<Integer> chunkIteratorList = new ArrayList<>();
        for (int t = 1; t <= threads; t++) {
            chunkIteratorList.add(t);
        }

        chunkIteratorList.parallelStream().forEach(chunkId -> {
            //Each thread will have its own em, so each em will have a smaller data package to process.
            EntityManager em = emf.createEntityManager();

            long chunkSize = (long) Math.ceil((double) total / (double) chunkIteratorList.size());
            long iStart = ((chunkId - 1) * chunkSize) + 1;
            long iEnd = Math.min(chunkSize * chunkId, total);

            em.getTransaction().begin();
            for (long i = iStart; i <= iEnd; i++) {
                //To further ease the em's job within the same thread, its data is split into multiple transactions.
                if (i % 20L == 0L) {
                    em.getTransaction().commit();
                    em.getTransaction().begin();
                    em.clear(); //Many transactions results in a lighter em. Gain:+
                }
                Person person = new Person();
                person.setName("Test_" + i);
                em.persist(person);
            }
            em.getTransaction().commit();

            em.close(); //Many threads results in a lighter em. Gain: ++++++
        });
    }
}
