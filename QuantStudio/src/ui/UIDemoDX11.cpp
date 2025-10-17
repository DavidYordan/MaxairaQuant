#include "UIDemoDX11.h"
#include <dxgi.h>
#include <tchar.h>
#include <imgui.h>
#include <implot.h>
#include <imgui_impl_win32.h>
#include <imgui_impl_dx11.h>

// 🔹 新版 ImGui 后端函数不再自动声明，需手动声明（1.91.9+）
extern IMGUI_IMPL_API LRESULT ImGui_ImplWin32_WndProcHandler(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);

namespace qs {

static void ComputeSine(float* xs, float* ys, int n) {
    for (int i = 0; i < n; ++i) {
        xs[i] = i * 0.01f;
        ys[i] = sinf(xs[i] * 2.0f);
    }
}

bool UIDemoDX11::CreateDeviceD3D(HWND hWnd) {
    DXGI_SWAP_CHAIN_DESC sd = {};
    sd.BufferCount = 2;
    sd.BufferDesc.Format = DXGI_FORMAT_R8G8B8A8_UNORM;
    sd.BufferUsage = DXGI_USAGE_RENDER_TARGET_OUTPUT;
    sd.OutputWindow = hWnd;
    sd.SampleDesc.Count = 1;
    sd.Windowed = TRUE;
    sd.SwapEffect = DXGI_SWAP_EFFECT_DISCARD;

    D3D_FEATURE_LEVEL featureLevel;
    const D3D_FEATURE_LEVEL featureLevelArray[3] = {
        D3D_FEATURE_LEVEL_11_0, D3D_FEATURE_LEVEL_10_1, D3D_FEATURE_LEVEL_10_0
    };

    HRESULT hr = D3D11CreateDeviceAndSwapChain(
        nullptr, D3D_DRIVER_TYPE_HARDWARE, nullptr, 0, featureLevelArray, 3,
        D3D11_SDK_VERSION, &sd, &swap_chain_, &device_, &featureLevel, &device_ctx_);
    if (FAILED(hr)) return false;

    CreateRenderTarget();
    return true;
}

void UIDemoDX11::CreateRenderTarget() {
    ID3D11Texture2D* pBackBuffer = nullptr;
    swap_chain_->GetBuffer(0, __uuidof(ID3D11Texture2D), (void**)&pBackBuffer);
    device_->CreateRenderTargetView(pBackBuffer, nullptr, &rtv_);
    if (pBackBuffer) pBackBuffer->Release();
}

void UIDemoDX11::CleanupRenderTarget() {
    if (rtv_) { rtv_->Release(); rtv_ = nullptr; }
}

void UIDemoDX11::CleanupDeviceD3D() {
    CleanupRenderTarget();
    if (swap_chain_) { swap_chain_->Release(); swap_chain_ = nullptr; }
    if (device_ctx_) { device_ctx_->Release(); device_ctx_ = nullptr; }
    if (device_) { device_->Release(); device_ = nullptr; }
}

LRESULT WINAPI UIDemoDX11::WndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam) {
    if (ImGui_ImplWin32_WndProcHandler(hWnd, msg, wParam, lParam))
        return true;

    auto* self = reinterpret_cast<UIDemoDX11*>(GetWindowLongPtr(hWnd, GWLP_USERDATA));
    switch (msg) {
    case WM_SIZE:
        if (self && self->device_ && wParam != SIZE_MINIMIZED) {
            self->CleanupRenderTarget();
            self->swap_chain_->ResizeBuffers(0, (UINT)LOWORD(lParam), (UINT)HIWORD(lParam), DXGI_FORMAT_UNKNOWN, 0);
            self->CreateRenderTarget();
        }
        return 0;
    case WM_DESTROY:
        PostQuitMessage(0);
        return 0;
    default:
        return DefWindowProc(hWnd, msg, wParam, lParam);
    }
}

bool UIDemoDX11::init(const std::wstring& title, int width, int height) {
    WNDCLASSEXW wc = {};
    wc.cbSize = sizeof(wc);
    wc.style = CS_CLASSDC;
    wc.lpfnWndProc = UIDemoDX11::WndProc;
    wc.hInstance = GetModuleHandle(nullptr);
    wc.lpszClassName = L"QuantStudioDX11Class";
    RegisterClassExW(&wc);
    hwnd_ = CreateWindowW(wc.lpszClassName, title.c_str(),
                          WS_OVERLAPPEDWINDOW, 100, 100, width, height,
                          nullptr, nullptr, wc.hInstance, nullptr);
    if (!CreateDeviceD3D(hwnd_)) {
        CleanupDeviceD3D();
        UnregisterClassW(wc.lpszClassName, wc.hInstance);
        return false;
    }
    SetWindowLongPtr(hwnd_, GWLP_USERDATA, (LONG_PTR)this);
    ShowWindow(hwnd_, SW_SHOW);
    UpdateWindow(hwnd_);

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImPlot::CreateContext();
    ImGui::StyleColorsDark();

    // 🔹 启用 Docking 支持
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_DockingEnable;

    ImGui_ImplWin32_Init(hwnd_);
    ImGui_ImplDX11_Init(device_, device_ctx_);
    return true;
}

void UIDemoDX11::main_loop() {
    // --- 静态演示数据 ---
    static float xs_demo[1000], ys_demo[1000];
    ComputeSine(xs_demo, ys_demo, 1000);

    const int max_points = 300; // 最多保留 300 个点（窗口大小）
    MSG msg{};
    while (msg.message != WM_QUIT) {
        if (PeekMessage(&msg, nullptr, 0U, 0U, PM_REMOVE)) {
            TranslateMessage(&msg);
            DispatchMessage(&msg);
            continue;
        }

        ImGui_ImplDX11_NewFrame();
        ImGui_ImplWin32_NewFrame();
        ImGui::NewFrame();

        // --- DockSpace 全局布局 ---
        ImGui::DockSpaceOverViewport(0, ImGui::GetMainViewport(), ImGuiDockNodeFlags_None, nullptr);

        // --- Market 图表 ---
        ImGui::Begin("Market");
        if (store_) {
            auto snap = store_->snapshot();
            if (!snap.empty()) {
                // 仅保留最近 N 个点
                size_t n = snap.size();
                if (n > max_points) {
                    snap.erase(snap.begin(), snap.end() - max_points);
                    n = max_points;
                }

                std::vector<float> xs(n);
                std::vector<float> ys(n);
                double t0 = snap.front().t;
                for (size_t i = 0; i < n; ++i) {
                    xs[i] = static_cast<float>(snap[i].t - t0); // 相对时间（秒）
                    ys[i] = static_cast<float>(snap[i].v);
                }

                if (ImPlot::BeginPlot("BTCUSDT Mark Price")) {
                    ImPlot::SetupAxes("Time (s)", "Price (USD)", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
                    ImPlot::SetupAxisLimits(ImAxis_X1, xs.front(), xs.back(), ImGuiCond_Always);
                    ImPlot::PlotLine("MarkPrice", xs.data(), ys.data(), (int)n);
                    ImPlot::EndPlot();
                }

                ImGui::Text("Last: %.2f USD", ys.back());
                ImGui::SameLine();
                ImGui::Text("Points: %zu", n);
            } else {
                ImGui::Text("Waiting for Binance data...");
            }
        } else {
            ImGui::Text("DataStore not initialized.");
        }
        ImGui::End();

        // --- Demo 图 ---
        ImGui::Begin("UI Demo");
        ImGui::Text("DX11 + ImGui + ImPlot minimal demo (Docking on)");
        if (ImPlot::BeginPlot("Sine Wave")) {
            ImPlot::PlotLine("sin", xs_demo, ys_demo, 1000);
            ImPlot::EndPlot();
        }
        ImGui::End();

        // --- 渲染 ---
        ImGui::Render();
        const float clear_color[4] = {0.10f, 0.10f, 0.12f, 1.0f};
        device_ctx_->OMSetRenderTargets(1, &rtv_, nullptr);
        device_ctx_->ClearRenderTargetView(rtv_, clear_color);
        ImGui_ImplDX11_RenderDrawData(ImGui::GetDrawData());
        swap_chain_->Present(1, 0);
    }
}

void UIDemoDX11::shutdown() {
    ImGui_ImplDX11_Shutdown();
    ImGui_ImplWin32_Shutdown();
    ImPlot::DestroyContext();
    ImGui::DestroyContext();

    CleanupDeviceD3D();
    if (hwnd_) {
        DestroyWindow(hwnd_);
        hwnd_ = nullptr;
    }
    UnregisterClassW(L"QuantStudioDX11Class", GetModuleHandle(nullptr));
}

} // namespace qs
