package com.maxaira.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class ClientBroadcastService {

    private final Map<String, Set<WebSocketSession>> subscriptions = new ConcurrentHashMap<>();
    private final Map<WebSocketSession, Set<String>> sessionKeys = new ConcurrentHashMap<>();

    public static String keyOf(String symbol, String market, String period) {
        return symbol.toUpperCase() + "|" + market.toLowerCase() + "|" + period.toLowerCase();
    }

    public void subscribe(WebSocketSession session, String symbol, String market, String period) {
        String key = keyOf(symbol, market, period);
        subscriptions.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(session);
        sessionKeys.computeIfAbsent(session, s -> ConcurrentHashMap.newKeySet()).add(key);
        log.info("Client subscribed: {} -> {}", session.getId(), key);
    }

    public void unsubscribe(WebSocketSession session, String symbol, String market, String period) {
        String key = keyOf(symbol, market, period);
        Set<WebSocketSession> set = subscriptions.get(key);
        if (set != null) {
            set.remove(session);
            if (set.isEmpty()) {
                subscriptions.remove(key);
            }
        }
        Set<String> keys = sessionKeys.get(session);
        if (keys != null) {
            keys.remove(key);
        }
        log.info("Client unsubscribed: {} -> {}", session.getId(), key);
    }

    public void remove(WebSocketSession session) {
        Set<String> keys = sessionKeys.remove(session);
        if (keys != null) {
            for (String key : keys) {
                Set<WebSocketSession> set = subscriptions.get(key);
                if (set != null) {
                    set.remove(session);
                    if (set.isEmpty()) {
                        subscriptions.remove(key);
                    }
                }
            }
        }
        log.info("Client disconnected: {}", session.getId());
    }

    public void broadcastRaw(String symbol, String market, String period, String text) {
        String key = keyOf(symbol, market, period);
        Set<WebSocketSession> set = subscriptions.get(key);
        if (set == null || set.isEmpty()) return;

        for (WebSocketSession session : set) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(text));
                }
            } catch (Exception e) {
                log.warn("Broadcast failed to session {}: {}", session.getId(), e.getMessage());
            }
        }
    }
}