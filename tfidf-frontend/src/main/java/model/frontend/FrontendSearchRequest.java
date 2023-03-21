package model.frontend;

public class FrontendSearchRequest {// ui에서 서버로 보낼 요청

    private String searchQuery;

    private Long maxNumberOfResults = Long.MAX_VALUE;

    private double minScore = 0.0;

    public String getSearchQuery() {
        return searchQuery;
    }

    public Long getMaxNumberOfResults() {
        return maxNumberOfResults;
    }

    public double getMinScore() {
        return minScore;
    }
}
