package vehicles;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.java_websocket.handshake.ServerHandshake;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import javax.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;

// from https://gist.github.com/tonvanbart/17dc93be413f7c53b76567e10b87a141
@Slf4j
public class WebSocketStream extends RichSourceFunction<String> {
    private boolean running = true;
    transient AsyncHttpClient client;
    transient BoundRequestBuilder boundRequestBuilder;
    transient WebSocketUpgradeHandler.Builder webSocketListener;
    private BlockingQueue<String> messages = new ArrayBlockingQueue<>(100);

    private final String websocketURI = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326";

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        WebSocketUpgradeHandler webSocketUpgradeHandler = webSocketListener.addWebSocketListener(
                new WebSocketListener() {

                    private final ObjectMapper myMapper = new ObjectMapper();

                    private String getRsvpId(String payload) {
                        try {
                            Map map = myMapper.readValue(payload, Map.class);
                            Object rsvpId = map.get("rsvp_id");
                            return rsvpId != null ? rsvpId.toString() : "NOT FOUND";
                        } catch (IOException e) {
                            log.error("Mapping failed, returning 'null'");
                            return "NULL";
                        }
                    }

                    @Override
                    public void onOpen(WebSocket webSocket) {
                    }

                    @Override
                    public void onClose(WebSocket webSocket, int i, String s) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                        log.debug("onTextFrame({}), rsvp_id={}", hash(payload), getRsvpId(payload));
                        //System.out.println(payload);
                        if (payload != null) {
                            try {
                                messages.put(payload);
                            } catch (InterruptedException e) {
                                log.error("Interrupted!", e);
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }).build();
        boundRequestBuilder.execute(webSocketUpgradeHandler).get();

        while (running) {
            ctx.collect(messages.take());
        }
        running = false;
    }

    @Override
    public void cancel() {
        log.info("cancel function called");
        running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("open function called");
        super.open(parameters);
        client = Dsl.asyncHttpClient();
        boundRequestBuilder = client.prepareGet(websocketURI);
        webSocketListener = new WebSocketUpgradeHandler.Builder();
    }

    private String hash(String input) {
        if (input == null) {
            return "-- NULL --";
        }

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            log.error("Cound not instantiate MD5", e);
            return "--NOT CALCULATED--";
        }
    }
}
