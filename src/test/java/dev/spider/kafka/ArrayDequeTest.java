package dev.spider.kafka;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;

class ArrayDequeTest {

    @Test
    void testArrayDequeForPollLast(){
        Deque<Object> deque = new ArrayDeque<>();
        deque.push("a");
        deque.add("b");
        System.out.println(deque.pollFirst());
        System.out.println(deque.pollLast());
        Object poll = deque.poll();
        Object pop = deque.pop();
        Object element = deque.element();


        deque.add("c");
        deque.addLast("d");
        deque.add("e");
        Object o1 = deque.pollLast();
        System.out.println(o1);
        int size = deque.size();
        System.out.println(size);
        for (Object o : deque) {
            System.out.println(o);
        }
    }
}
