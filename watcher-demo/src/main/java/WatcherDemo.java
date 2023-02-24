/*
 *  MIT License
 *
 *  Copyright (c) 2019 Michael Pogrebinsky - Distributed Systems & Cloud Computing with Java
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    private static final int SESSION_TIMEOUT = 3000;

    private static final String TARGET_ZNODE = "/target_znode";

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        WatcherDemo watchersDemo = new WatcherDemo();

        watchersDemo.connectToZookeeper();

        watchersDemo.watchTargetZNode();

        watchersDemo.run();

        watchersDemo.close();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void watchTargetZNode() throws InterruptedException, KeeperException {
        Stat stat=zooKeeper.exists(TARGET_ZNODE,this);

        // zNode가 존재하지 않으면 바로 종료
        if (stat==null){
            return;
        }

        byte [] data=zooKeeper.getData(TARGET_ZNODE,this,stat);

        List<String> children=zooKeeper.getChildren(TARGET_ZNODE,this);

        System.out.println("Data :"+new String(data) +" children : "+children);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    // 이벤트의 종류에 따라 핸들링해줌.
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE+" was deleted");
                break;

            case NodeCreated:
                System.out.println(TARGET_ZNODE+" was created");
                break;
            case NodeDataChanged: //getData() 호출 시
                System.out.println(TARGET_ZNODE+ "data changed");
                break;
            case NodeChildrenChanged://getChildren() 호출 시
                System.out.println(TARGET_ZNODE+" children changed");
                break;
        }
        try{
            watchTargetZNode();// 변경이 발생했을 때 모든 데이터를 최신으로 화면에 출력하기 위함. 그리고 일회용 트리거를 다시 등록하기 위함.
        }catch (KeeperException | InterruptedException e){

        }
    }
}
