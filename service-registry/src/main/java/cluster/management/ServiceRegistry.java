package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String REGISTRY_ZNODE="/service_registry";

    private final ZooKeeper zooKeeper;

    private String currentZnode=null;

    private List<String> allServiceAddress=null;

    public ServiceRegistry(ZooKeeper zooKeeper){
        this.zooKeeper=zooKeeper;
        createServiceRegistryZnode();
    }

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

    private synchronized  void updateAddress() throws InterruptedException, KeeperException {
        List<String>workerZnodes=zooKeeper.getChildren(REGISTRY_ZNODE,this);

        List<String>addresses=new ArrayList<>(workerZnodes.size());

        for(String workerZnode:workerZnodes){
            String workerZnodeFullPath=REGISTRY_ZNODE+"/"+workerZnode;
            Stat stat=zooKeeper.exists(workerZnodeFullPath,false);
            if(stat==null){
                continue;
            }

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
            updateAddress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
