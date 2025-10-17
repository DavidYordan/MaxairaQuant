package com.maxaira.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxaira.service.WebSocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.*;

import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClientWebSocketHandler extends TextWebSocketHandler {

    private final ClientBroadcastService broadcastService;
    private final WebSocketService upstreamWs;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void afterConnectionEstablished(@NonNull WebSocketSession session) {
        log.info("Client connected: {}", session.getId());
    }

    @Override
    public void handleTextMessage(@NonNull WebSocketSession session, @NonNull TextMessage message) {
        try {
            JsonNode root = mapper.readTree(message.getPayload());
            String action = root.path("action").asText("").toLowerCase();
            String symbol = root.path("symbol").asText("");
            String market = root.path("market").asText("");
            String period = root.path("period").asText("1m");

            switch (action) {
                case "subscribe" -> {
                    if (symbol.isEmpty() || market.isEmpty()) {
                        session.sendMessage(new TextMessage("{\"success\":false,\"msg\":\"symbol/market required\"}"));
                        return;
                    }
                    broadcastService.subscribe(session, symbol, market, period);
                    upstreamWs.start(symbol, market, period); // 确保上游订阅已启动
                    session.sendMessage(new TextMessage("{\"success\":true,\"action\":\"subscribe\"}"));
                }
                case "unsubscribe" -> {
                    if (symbol.isEmpty() || market.isEmpty()) {
                        session.sendMessage(new TextMessage("{\"success\":false,\"msg\":\"symbol/market required\"}"));
                        return;
                    }
                    broadcastService.unsubscribe(session, symbol, market, period);
                    session.sendMessage(new TextMessage("{\"success\":true,\"action\":\"unsubscribe\"}"));
                }
                case "ping" -> session.sendMessage(new TextMessage("{\"pong\":true}"));
                default -> session.sendMessage(new TextMessage("{\"success\":false,\"msg\":\"unknown action\"}"));
            }
        } catch (Exception e) {
            log.error("Client message handling failed: {}", e.getMessage(), e);
            try {
                session.sendMessage(new TextMessage("{\"success\":false,\"msg\":\"invalid payload\"}"));
            } catch (Exception ignored) {}
        }
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession session, @NonNull CloseStatus status) {
        broadcastService.remove(session);
        log.info("Client disconnected: {} ({})", session.getId(), status);
    }
}