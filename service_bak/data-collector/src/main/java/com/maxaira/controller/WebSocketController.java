package com.maxaira.controller;

import com.maxaira.service.WebSocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/websocket")
@RequiredArgsConstructor
public class WebSocketController {
    
    private final WebSocketService webSocketService;
    
    @GetMapping("/status")
    public ResponseEntity<?> getWebSocketStatus() {
        try {
            Map<String, Object> status = webSocketService.status();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "data", status
            ));
            
        } catch (Exception e) {
            log.error("获取 WebSocket 状态失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", "获取状态失败: " + e.getMessage()
            ));
        }
    }
}