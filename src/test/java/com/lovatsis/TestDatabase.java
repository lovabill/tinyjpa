package com.lovatsis;

import com.lovatsis.model.Person;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

public class TestDatabase {

    private EntityManager em;

    @Before
    public void start() {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("primary");
        em = emf.createEntityManager();

        em.getTransaction().begin();

        Person person = new Person();
        person.setName("Test");
        em.persist(person);

        em.getTransaction().commit();
    }

    @Test
    public void testData() {
        Person person = em.createQuery("SELECT p FROM Person p", Person.class).getSingleResult();
        Assert.assertNotNull(person);
        Assert.assertEquals("Test", person.getName());
    }

    @After
    public void finish() {
        em.close();
    }
}
