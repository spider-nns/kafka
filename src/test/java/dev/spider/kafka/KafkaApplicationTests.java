package dev.spider.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaApplicationTests {

    @Test
    void contextLoads() {
        System.out.println(3 & 4);
        // 0101
        // 0110
        // 0100 4

        // 0011
        // 0100
        // 0
        System.out.println(3 & 15);
        System.out.println((-1) & 7);
        System.out.println(Integer.toBinaryString(-1));
        System.out.println(Integer.toBinaryString(-23));
        System.out.println(Integer.toBinaryString(-5));
        System.out.println(0b11111011);
        System.out.println(0b10000101);
        System.out.println(0b11111111111111111111111111111011);
//        System.out.println(Integer.valueOf("-5",2));
        System.out.println(Integer.toBinaryString(133));
        System.out.println(Integer.toBinaryString(-133));
        System.out.println(Integer.toBinaryString(-8));
        System.out.println(Integer.toBinaryString(-7));
        System.out.println(0b11111111111111111111111111111010);
        System.out.println(Integer.toHexString(7));
        System.out.println(Integer.toHexString(-7));
        System.out.println(0x7);
    }
}
