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

        // Split the job in parallel threads (partitions)
        List<Integer> partitions = new ArrayList<>();
        for (int t = 1; t <= threads; t++) {
            partitions.add(t);
        }

        partitions.parallelStream().forEach(partitionId -> {
            // Each thread will process a smaller amount of data and will have its own em,
            // so each em will have a smaller data package to process.
            EntityManager em = emf.createEntityManager();

            //Each partition will handle a specific subset of the total data
            long partitionSize = (long) Math.ceil((double) total / (double) partitions.size());
            long iStart = ((partitionId - 1) * partitionSize) + 1;
            long iEnd = Math.min(partitionSize * partitionId, total);

            em.getTransaction().begin();
            for (long i = iStart; i <= iEnd; i++) {
                //Each partition is further divided into chunks in order to
                //a. split the transaction into multiple smaller ones to avoid huge rollbacks
                //b. increase the persistence performance of the em
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
