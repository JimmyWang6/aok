package com.aok.meta.container;

import com.aok.meta.Queue;
import com.aok.meta.Vhost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryMetaContainerTest {
    
    InMemoryMetaContainer inMemoryMetaContainer;
    
    @BeforeEach
    public void setUp() {
        inMemoryMetaContainer = new InMemoryMetaContainer();
    }

    @Test
    public void testAddAndGet() {
        String vhost1 = "testVhost";
        String nameQ1 = "testName";
        String nameQ2 = "testName2";
        Queue q1 = new Queue(vhost1, nameQ1, false, false, true, null);
        Queue q2 = new Queue(vhost1, nameQ2, false, false, true, null);
        inMemoryMetaContainer.add(q1);
        inMemoryMetaContainer.add(q2);
        Queue getQueue1 = (Queue) inMemoryMetaContainer.get(Queue.class, vhost1, nameQ1);
        Queue getQueue2 = (Queue) inMemoryMetaContainer.get(Queue.class, vhost1, nameQ2);
        assertEquals(q1, getQueue1);
        assertEquals(q2, getQueue2);
        assertEquals(2, inMemoryMetaContainer.list(Queue.class).size());
        Vhost vhostA = new Vhost(vhost1);
        inMemoryMetaContainer.add(vhostA);
        Vhost VhostB = (Vhost) inMemoryMetaContainer.get(Vhost.class, vhost1, vhost1);
        assertEquals(vhostA, VhostB);
        assertEquals(1, inMemoryMetaContainer.list(Vhost.class).size());
        assertEquals(2, inMemoryMetaContainer.list(Queue.class).size());
        inMemoryMetaContainer.delete(vhostA);
        assertEquals(0, inMemoryMetaContainer.list(Vhost.class).size());
    }
}
