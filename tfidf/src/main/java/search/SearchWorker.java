package search;

import networking.OnRequestCallback;

public class SearchWorker implements OnRequestCallback {
    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        return new byte[0];
    }

    @Override
    public String getEndpoint() {
        return null;
    }
}
