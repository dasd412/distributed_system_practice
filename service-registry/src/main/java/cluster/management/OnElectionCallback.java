package cluster.management;

import org.apache.zookeeper.KeeperException;

//서비스 레지스트리와 서비스 디스커버리를 담당하는 ServiceRegistry와 리더 선출만 담당하는 LeaderElection을 분리하기 위한 콜백 인터페이스
public interface OnElectionCallback {
    void onElectedToBeLeader() throws InterruptedException, KeeperException;

    void onWorker();
}
