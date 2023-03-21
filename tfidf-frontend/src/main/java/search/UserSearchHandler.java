package search;

import cluster.management.ServiceRegistry;
import networking.OnRequestCallback;

public class UserSearchHandler implements OnRequestCallback {

    public UserSearchHandler(ServiceRegistry coordinatorsServiceRegistry) {
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        return new byte[0];
    }

    @Override
    public String getEndpoint() {
        return null;
    }
}
