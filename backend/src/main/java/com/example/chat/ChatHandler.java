package com.example.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protocol (JSON over WebSocket):
 *
 * client -> server
 *   {"type":"join","name":"Asha"}
 *   {"type":"message","text":"hello"}
 *
 * server -> client
 *   {"type":"history","messages":[...]}
 *   {"type":"message","name":"Asha","text":"hello","time":"2026-01-01T10:00:00Z"}
 *   {"type":"system","text":"Asha joined","time":"..."}
 *   {"type":"users","users":["Asha","Ravi"]}
 */
@Component
public class ChatHandler extends TextWebSocketHandler {

    private static final int MAX_HISTORY = 50;
    private static final int MAX_TEXT = 500;
    private static final int MAX_NAME = 20;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, String> names = new ConcurrentHashMap<>();
    private final Deque<Map<String, Object>> history = new ArrayDeque<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        // Decorator makes concurrent sends to one session safe.
        sessions.put(session.getId(), new ConcurrentWebSocketSessionDecorator(session, 5000, 64 * 1024));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        JsonNode json;
        try {
            json = mapper.readTree(message.getPayload());
        } catch (Exception e) {
            return; // ignore malformed input
        }
        String type = json.path("type").asText("");
        WebSocketSession out = sessions.getOrDefault(session.getId(), session);

        if ("join".equals(type)) {
            String name = clean(json.path("name").asText(""), MAX_NAME);
            if (name.isEmpty()) {
                name = "Guest" + (int) (Math.random() * 900 + 100);
            }
            boolean firstJoin = names.put(session.getId(), name) == null;

            Map<String, Object> hist = new LinkedHashMap<>();
            hist.put("type", "history");
            hist.put("messages", snapshotHistory());
            send(out, hist);

            if (firstJoin) {
                broadcast(systemMessage(name + " joined"));
            }
            broadcastUsers();

        } else if ("message".equals(type)) {
            String name = names.get(session.getId());
            String text = clean(json.path("text").asText(""), MAX_TEXT);
            if (name == null || text.isEmpty()) {
                return;
            }
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("type", "message");
            msg.put("name", name);
            msg.put("text", text);
            msg.put("time", Instant.now().toString());
            remember(msg);
            broadcast(msg);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        String name = names.remove(session.getId());
        if (name != null) {
            broadcast(systemMessage(name + " left"));
            broadcastUsers();
        }
    }

    // ---- helpers ----

    private static String clean(String s, int max) {
        String t = s == null ? "" : s.trim();
        return t.length() > max ? t.substring(0, max) : t;
    }

    private Map<String, Object> systemMessage(String text) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "system");
        m.put("text", text);
        m.put("time", Instant.now().toString());
        return m;
    }

    private void remember(Map<String, Object> msg) {
        synchronized (history) {
            history.addLast(msg);
            while (history.size() > MAX_HISTORY) {
                history.removeFirst();
            }
        }
    }

    private List<Map<String, Object>> snapshotHistory() {
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }

    private void broadcastUsers() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "users");
        m.put("users", new ArrayList<>(names.values()));
        broadcast(m);
    }

    private void broadcast(Object payload) {
        String json;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (Exception e) {
            return;
        }
        for (WebSocketSession s : sessions.values()) {
            try {
                if (s.isOpen()) {
                    s.sendMessage(new TextMessage(json));
                }
            } catch (Exception ignored) {
                // a slow or broken client must not affect the others
            }
        }
    }

    private void send(WebSocketSession s, Object payload) {
        try {
            s.sendMessage(new TextMessage(mapper.writeValueAsString(payload)));
        } catch (Exception ignored) {
        }
    }
}