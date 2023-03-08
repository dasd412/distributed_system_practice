package networking;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class WebClient {

    private HttpClient httpClient;

    public WebClient(){
        this.httpClient=HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    public CompletableFuture<String> sendTask(String url, byte[] requestPayload){
        HttpRequest request=HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestPayload))
                .uri(URI.create(url))
                .build();


        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())// 비동기적으로 전송만 하고 응답은 기다리지 않는다.
                .thenApply(HttpResponse::body);//<- 실제 응답이 오면 헤더 없이 메시지 본문만 가져온다는 뜻
    }
}
