package com.zw.controller;

import com.zw.util.DistributedLockByCurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class TestController {
    @Autowired
    private DistributedLockByCurator lock;

    @GetMapping(value = "/getLockBlock/{id}")
    public Long getLockBlock(@PathVariable String id) throws InterruptedException {
        Long start = System.currentTimeMillis();
        String key = "lock"+id;
        lock.acquireDistributedLock(key);
        try {
            long l = new Random().nextInt(1000);
            System.out.println("sleep :"+l+"ms");
            Thread.sleep(l);
            lock.releaseDistributedLock(key);
            Long end = System.currentTimeMillis();
            long spend = end - start;
            System.out.println("spend :"+spend+"ms");
            return spend;
        } finally {
            lock.releaseDistributedLock(key);
        }

    }

    @GetMapping(value = "/getLock/{id}")
    public String getLock(@PathVariable String id) throws InterruptedException {
        Long start = System.currentTimeMillis();
        String key = "lock"+id;
        boolean getLock = lock.acquireDistributedLockNBLock(key);
        if (!getLock){
            return "get Lock failed";
        }
        try {
            long l = new Random().nextInt(1000);
            System.out.println("sleep :"+l+"ms");
            Thread.sleep(l);
            Long end = System.currentTimeMillis();
            long spend = end - start;
            System.out.println("spend :"+spend+"ms");
            return String.valueOf(spend);
        } finally {
            lock.releaseDistributedLock(key);
        }
    }
}
