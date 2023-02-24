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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderSelection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS="localhost:2181";

    //주키퍼는 클라이언트가 연결되었는지 지속적으로 확인한다.
    private static final int SESSION_TIMEOUT=3000; //<- ms 단위. 클라이언트가 연결되어 있는 지 확인하는 시간이다.

    private static final String ELECTION_NAMESPACE="/election";

    private ZooKeeper zooKeeper;

    private String currentZNodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderSelection leaderSelection=new LeaderSelection();

        leaderSelection.connectToZookeeper();

        leaderSelection.volunteerForLeadership();

        leaderSelection.reElectLeader();

        leaderSelection.run();

        leaderSelection.close();

        System.out.println("disconnected from zookeeper. exiting...");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        // 임시 zNode로 생성하면, 주키퍼 연결이 끊어질 경우 해당 z노드는 삭제된다.
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZNodeName = znodeFullPath.replace("/election/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {

        Stat predecessorStat=null;

        String predecessorZNodeName="";

        while(predecessorStat==null){//<- 레이스 컨디션 방지용

            List<String> children=zooKeeper.getChildren(ELECTION_NAMESPACE,false);

            Collections.sort(children);

            String smallestChildren=children.get(0);

            if(smallestChildren.equals(currentZNodeName)){
                System.out.println("I am leader");
                return;
            }else{
                System.out.println("I am not leader");
                //리더가 아니라면 바로 이전 순서의 노드를 확인
                int predecessorIndex=Collections.binarySearch(children,currentZNodeName)-1;
                predecessorZNodeName=children.get(predecessorIndex);
                predecessorStat=zooKeeper.exists(ELECTION_NAMESPACE+"/"+predecessorZNodeName,this);
            }
        }
        System.out.println("Watching znode "+predecessorZNodeName);

    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper=new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){
            case None:
                // 주키퍼 서버와 정상 연결된 경우
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("successfully connected to zookeeper");
                }
                else{
                    // 주키퍼 서버와 연결이 끊어지면, waiting 중인 스레드들을 wakeup한다.
                    // 그러면 이 코드의 run()이 중단되고 close()가 실행된다.
                    System.out.println("disconnected from zookeeper event");
                    zooKeeper.notifyAll();
                }
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
                break;
        }
    }
}
