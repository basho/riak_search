package raptor.store.handlers;

import org.json.*;

public abstract class ResultHandler {
    public void handleResult(byte[] key, byte[] value) { }
    public void handleResult(String key, String value) { }
    public void handleInfoResult(String bucket, long count) { }
    public void handleCatalogResult(JSONObject obj) { }
}

