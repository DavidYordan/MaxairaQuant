package com.maxaira.controller;

import com.maxaira.service.DataBufferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/buffer")
@RequiredArgsConstructor
public class DataBufferController {
    
    private final DataBufferService dataBufferService;
    
    @GetMapping("/status")
    public ResponseEntity<?> getBufferStatus() {
        try {
            Map<String, Object> status = dataBufferService.getBufferStatus();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "data", status
            ));
            
        } catch (Exception e) {
            log.error("获取缓冲区状态失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", "获取缓冲区状态失败: " + e.getMessage()
            ));
        }
    }
}