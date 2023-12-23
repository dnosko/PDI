/* Adapted from:  https://github.com/tonvanbart/flink-examples/blob/master/websocket-sourcefunction/src/main/java/org/vanbart/functions/WebSocketSourceFunction.java
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
        OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
        ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org>
*/
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
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;

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
