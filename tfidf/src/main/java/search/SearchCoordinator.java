package search;

import cluster.management.ServiceRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import model.DocumentData;
import model.Result;
import model.SerializationUtils;
import model.Task;
import model.proto.SearchModel;
import networking.OnRequestCallback;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestCallback {

    private static final String ENDPOINT = "/search";

    private static final String BOOKS_DIRECTORY = "./resources/books/";

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
        SearchModel.Request request = null;
        try {
            request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);

            return response.toByteArray();
        } catch (InvalidProtocolBufferException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance().toByteArray();
        }
    }

    private SearchModel.Response createResponse(SearchModel.Request saerchRequest) throws InterruptedException, KeeperException {
        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();

        System.out.println("Received search query: " + saerchRequest.getSearchQuery());

        List<String> searchTerms = TFIDF.getWordsFromLine(saerchRequest.getSearchQuery());

        List<String> workers = workerServiceRegistry.getAllServiceAddresses();

        //만약 이용가능한 워커 노드가 없을 경우의 예외 처리
        if (workers.isEmpty()) {
            System.out.println("No search workers currently available");
            return searchResponse.build();
        }

        //이용가능한 워커 노드가 있으면 작업을 만들고, 비동기 요청 전송
        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results, searchTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);

        return searchResponse.build();
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

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> terms) {
        Map<String, DocumentData> allDocumentResults = new HashMap<>();

        for (Result result : results) {
            allDocumentResults.putAll(result.getDocumentToDocumentData());
        }

        System.out.println("calculating score for all the documents");

        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentsScore(terms, allDocumentResults);

        return sortDocumentsByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> sortDocumentsByScore(Map<Double, List<String>> scoreToDocuments) {
        List<SearchModel.Response.DocumentStats> sortedDocumentsStatsList = new ArrayList<>();

        for (Map.Entry<Double, List<String>> docScorePair : scoreToDocuments.entrySet()) {
            double score = docScorePair.getKey();

            for (String document : docScorePair.getValue()) {
                File documentPath = new File(document);

                SearchModel.Response.DocumentStats documentStats = SearchModel.Response.DocumentStats.newBuilder()
                        .setScore(score)
                        .setDocumentName(documentPath.getName())
                        .setDocumentSize(documentPath.length())
                        .build();

                sortedDocumentsStatsList.add(documentStats);
            }
        }

        return sortedDocumentsStatsList;
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
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
