package model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

//워커 노드에게 보내기 위해 직렬화해야 함.
public class DocumentData implements Serializable {

    private Map<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String term, double frequency) {
        termToFrequency.put(term, frequency);
    }

    public double getFrequency(String term) {
        return this.termToFrequency.get(term);
    }
}
