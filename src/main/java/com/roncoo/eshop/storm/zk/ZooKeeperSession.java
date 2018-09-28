package com.roncoo.eshop.storm.zk;

import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author rinbo
 */
@Slf4j
public class ZooKeeperSession {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zookeeper;

    private ZooKeeperSession() {
        try {
            this.zookeeper = new ZooKeeper(
                    "192.168.208.129:2181,192.168.208.130:2181,192.168.208.131:2181",
                    50000,
                    new ZooKeeperWatcher());
            try {
                connectedSemaphore.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取分布式锁
     */
    public void acquireDistributedLock() {
        String path = "/taskid-list-lock";
        try {
            zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.debug("success to acquire lock for taskid-list-lock");
        } catch (Exception e) {
            int count = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                    zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e2) {
                    count++;
                    log.debug("the {} times try to acquire lock for taskid-list-lock......", count);
                    continue;
                }
                log.debug("success to acquire lock for taskid-list-lock after {} times try......", count);
                break;
            }
        }
    }

    /**
     * 释放掉一个分布式锁
     */
    public void releaseDistributedLock() {
        String path = "/taskid-list-lock";
        try {
            zookeeper.delete(path, -1);
            log.debug("release the lock for taskid-list-lock......");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getNodeData() {
        try {
            return new String(zookeeper.getData("/taskid-list", false, new Stat()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setNodeData(String path, String data) {
        try {
            zookeeper.setData(path, data.getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createNode(String path) {
        try {
            zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {

        }
    }

    /**
     * 建立zk session的watcher
     *
     * @author rinbo
     */
    private class ZooKeeperWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (KeeperState.SyncConnected == event.getState()) {
                connectedSemaphore.countDown();
            }
        }
    }

    private static class Singleton {

        private static ZooKeeperSession instance;

        static {
            instance = new ZooKeeperSession();
        }

        public static ZooKeeperSession getInstance() {
            return instance;
        }

    }

    public static ZooKeeperSession getInstance() {
        return Singleton.getInstance();
    }
    public static void init() {
        getInstance();
    }
}
