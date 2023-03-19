package model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

//검색 클러스터 코디네이터가 워커 노드에게 작업을 나타내는 데이터 모델. 네트워크 상에서 보내야 하므로 직렬화.
public class Task implements Serializable {

    private final List<String> searchTerms;

    private final List<String> documents;

    public Task(List<String> searchTerms, List<String> documents) {
        this.searchTerms = searchTerms;
        this.documents = documents;
    }

    public List<String> getSearchTerms() {
        return Collections.unmodifiableList(searchTerms);
    }

    public List<String> getDocuments() {
        return Collections.unmodifiableList(documents);
    }
}
