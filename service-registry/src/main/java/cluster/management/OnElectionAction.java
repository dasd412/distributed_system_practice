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
package cluster.management;

import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback{

    private final ServiceRegistry serviceRegistry;

    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port){
        this.serviceRegistry=serviceRegistry;
        this.port=port;
    }

    @Override
    public void onElectedToBeLeader() throws InterruptedException, KeeperException {
        //만약 워커에서 리더로 승격된 경우에는 서비스 레지스트리에 주소가 담겨져 있다. 그럴 경우 자기 자신과 통신하는 불상사가 발생할 수 있다.
        // 그러한 불상사를 해결하기 위해 서비스 레지스트리에서 자신의 주소를 제거한다.
        serviceRegistry.unregisterFromCluster();

        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try{
            // 워커 노드라면, 자신의 주소를 서비스 레지스트리에 등록한다. 해당 주소는 서비스 레지스트리 내 임시 z노드에 저장된다.
            String currentServerAddress=String.format("http://%s%d", InetAddress.getLocalHost().getCanonicalHostName(),port);
            serviceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
