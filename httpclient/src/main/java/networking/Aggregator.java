package networking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {// 비동기로 온 결과를 취합해줌

    private WebClient webClient;

    public Aggregator(){
        this.webClient=new WebClient();
    }

    public List<String> sendTasksToWorkers(List<String>workersAddresses,List<String>tasks){
        // 워커 서버에서 온 http 응답 결과를 저장
        CompletableFuture<String>[] futures=new CompletableFuture[workersAddresses.size()];

        for (int i = 0; i <workersAddresses.size(); i++) {
            String workerAddress=workersAddresses.get(i);
            String task=tasks.get(i);

            byte[]requestPayload=task.getBytes();

            futures[i]=webClient.sendTask(workerAddress,requestPayload);
        }

        List<String> results= Stream.of(futures)
                .map(CompletableFuture::join)// 해당하는 요청의 응답을 기다렸다가 받는다.
                .collect(Collectors.toList());  //모든 워커로부터 응답을 받고 나면 취합된 응답 반환

        return results;
    }
}
