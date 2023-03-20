package search;

import cluster.management.ServiceRegistry;
import model.Result;
import model.SerializationUtils;
import model.Task;
import networking.OnRequestCallback;
import networking.WebClient;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestCallback {

    private static final String ENDPOINT = "/search";

    private static final String BOOKS_DIRECTORY = "./resources/book";

    private final ServiceRegistry workerServiceRegistry;

    private final WebClient webClient;

    private final List<String> documents;

    public SearchCoordinator(ServiceRegistry workersServiceRegistry, WebClient webClient) {
        this.workerServiceRegistry = workersServiceRegistry;
        this.webClient = webClient;
        this.documents = readDocumentsList();
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        return new byte[]{};
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks) {
        //검색 워커 노드들에게 비동기적으로 요청 전송
        CompletableFuture<Result>[] futures = new CompletableFuture[workers.size()];

        for (int i = 0; i < workers.size(); i++) {
            String worker = workers.get(i);
            Task task = tasks.get(i);

            byte[] payload = SerializationUtils.serialize(task);

            futures[i] = webClient.sendTask(worker, payload);
        }

        //검색 워커 노드들로부터 모든 응답이 올때까지 응답 수집
        List<Result> results = new ArrayList<>();
        for (CompletableFuture<Result> future : futures) {
            try {
                Result result = future.get();
                results.add(result);
            } catch (InterruptedException | ExecutionException e) {
            }
        }

        //응답이 다 오면 결과를 취합해서  리턴.
        System.out.println(String.format("Received %d/%d results", results.size(), tasks.size()));
        return results;
    }

    //검색 워커에게 보낼 태스크 만들기
    public List<Task> createTasks(int numberOfWorkers, List<String> searchTerms) {
        List<List<String>> workersDocuments = splitDocumentList(numberOfWorkers, documents);

        List<Task> tasks = new ArrayList<>();

        for (List<String> documentsForWorker : workersDocuments) {
            Task task = new Task(searchTerms, documentsForWorker);
            tasks.add(task);
        }
        return tasks;
    }

    //디렉토리에서 읽어온 문서들을 각 워커 노드에게 나눠주는 메서드
    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents) {
        int numberOfDocumentsPerWorker = (documents.size() + numberOfWorkers - 1) / numberOfWorkers;

        List<List<String>> workersDocuments = new ArrayList<>();

        for (int i = 0; i < numberOfWorkers; i++) {
            int firstDocumentIndex = i * numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, documents.size());

            if (firstDocumentIndex >= lastDocumentIndexExclusive) {
                break;
            }

            List<String> currentWorkerDocuments = new ArrayList<>(documents.subList(firstDocumentIndex, lastDocumentIndexExclusive));
            workersDocuments.add(currentWorkerDocuments);
        }

        return workersDocuments;
    }

    private static List<String> readDocumentsList() {
        File documentDirectory = new File(BOOKS_DIRECTORY);
        return Arrays.asList(documentDirectory.list())
                .stream()
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }
}
