package raptor.store.handlers;

import org.json.JSONObject;

public abstract class ResultHandler {
    public void handleResult(byte[] key, byte[] value) {
    }

    public void handleResult(byte[] key, byte[] value, byte[] key_clock) {
    }

    public void handleInfoResult(String bucket, long count) {
    }

    public void handleCatalogResult(JSONObject obj) {
    }
}

