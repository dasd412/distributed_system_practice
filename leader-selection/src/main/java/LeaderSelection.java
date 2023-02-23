import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class LeaderSelection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS="localhost:2181";

    //주키퍼는 클라이언트가 연결되었는지 지속적으로 확인한다.
    private static final int SESSION_TIMEOUT=3000; //<- ms 단위. 클라이언트가 연결되어 있는 지 확인하는 시간이다.

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderSelection leaderSelection=new LeaderSelection();

        leaderSelection.connectToZookeeper();

        leaderSelection.run();

        leaderSelection.close();

        System.out.println("disconnected from zookeeper. exiting...");
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
        }
    }
}
