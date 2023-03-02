package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {// 서비스 레지스트리와 서비스 디스커버리 역할을 동시에 하는 클래스다.

    private static final String REGISTRY_ZNODE="/service_registry";

    private final ZooKeeper zooKeeper;

    private String currentZnode=null;

    private List<String> allServiceAddress=null;

    public ServiceRegistry(ZooKeeper zooKeeper){
        this.zooKeeper=zooKeeper;
        createServiceRegistryZnode();
    }

    //워커 노드의 주소를 서비스 레지스트리에 등록하기 위한 메서드
    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode=zooKeeper.create(REGISTRY_ZNODE+"/n",metadata.getBytes()
        ,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    private void createServiceRegistryZnode(){
        try{
            if(zooKeeper.exists(REGISTRY_ZNODE,false)==null){
                zooKeeper.create(REGISTRY_ZNODE,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }catch (KeeperException e){// race condition으로 둘다 위의 exists() 호출해서 null을 얻으면 이 블록으로 점프
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    // 여러개의 스레드가 동시에 변경을 일으킬 수 있으므로 synchronized 키워드를 추가한다.
    private synchronized  void updateAddress() throws InterruptedException, KeeperException {
        // 서비스 레지스트리 z 노드 안의 자식목록을 가져오고, NodeChildrenChanged 이벤트가 발생하면 이 클래스한테 알림이 간다.
        List<String>workerZnodes=zooKeeper.getChildren(REGISTRY_ZNODE,this);

        List<String>addresses=new ArrayList<>(workerZnodes.size());

        for(String workerZnode:workerZnodes){
            String workerZnodeFullPath=REGISTRY_ZNODE+"/"+workerZnode;
            Stat stat=zooKeeper.exists(workerZnodeFullPath,false);
            if(stat==null){// 위 코드의 zooKeeper.getChildren(REGISTRY_ZNODE,this)과 zooKeeper.exists(workerZnodeFullPath,false) 사이에 고장이 발생해서 z노드가 삭제되어 있을 수 있다.
                // 그러면 리턴 값인 Stat은 null이 된다. 이 경우에는 해당 주소를 addresses에 담을 필요가 없으므로 스킵한다.
                continue;
            }

            //고장나지 않은 정상 노드들에 대해선 각 z노드에 저장되어 있는 데이터( == 노드의 주소 값)을 가져오면 된다.
            byte[]addressBytes=zooKeeper.getData(workerZnodeFullPath,false,stat);
            String address=new String(addressBytes);
            addresses.add(address);
        }

        this.allServiceAddress= Collections.unmodifiableList(addresses);

        System.out.println("this cluster addresses are : "+this.allServiceAddress);

    }

    public void registerForUpdates(){
        try{
            updateAddress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    //노드가 스스로 종료되거나, 워커 노드가 갑자기 리더 노드가 될 경우 자기 자신과 통신하는 일을 피하기 위해 클러스터에서 제거하는 메서드.
    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if(currentZnode!=null&&zooKeeper.exists(currentZnode,false)!=null){
            zooKeeper.delete(currentZnode,-1);
        }
    }

    public synchronized  List<String>getAllServiceAddress() throws InterruptedException, KeeperException {
        if(allServiceAddress==null){
            updateAddress();
        }
        return allServiceAddress;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            // 이벤트에 대한 분기처리 없이, 이벤트가 발생하면 주소를 업데이트하도록 하였다.
            // 왜냐하면 감시하고 있던 노드가 고장나서 z노드가 NodeDeleted 됬던, 새로운 노드가 추가되서 새로운 zNode가 NodeCreated 됬던, 자식 목록이 변경됬던 간에 모두 주소를 갱신해야 하는 상황이기 때문이다.
            updateAddress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
