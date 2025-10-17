package com.maxaira.config;

import com.maxaira.websocket.ClientWebSocketHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.socket.config.annotation.*;

@Configuration
@EnableWebSocket
@RequiredArgsConstructor
public class WebSocketServerConfig implements WebSocketConfigurer {

    private final ClientWebSocketHandler clientHandler;

    @Override
    public void registerWebSocketHandlers(@NonNull WebSocketHandlerRegistry registry) {
        registry.addHandler(clientHandler, "/ws/stream")
                .setAllowedOrigins("*"); // 如需安全限制，可替换具体域名
    }
}