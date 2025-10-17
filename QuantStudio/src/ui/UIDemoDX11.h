#pragma once
#include <windows.h>
#include <d3d11.h>
#include <string>
#include "data/DataStore.h"

namespace qs {

class UIDemoDX11 {
public:
    bool init(const std::wstring& title, int width, int height);
    void main_loop();
    void shutdown();
    void setDataStore(DataStore* store) { store_ = store; }

private:
    HWND hwnd_ = nullptr;
    ID3D11Device* device_ = nullptr;
    ID3D11DeviceContext* device_ctx_ = nullptr;
    IDXGISwapChain* swap_chain_ = nullptr;
    ID3D11RenderTargetView* rtv_ = nullptr;
    DataStore* store_ = nullptr;

    bool CreateDeviceD3D(HWND hWnd);
    void CreateRenderTarget();
    void CleanupRenderTarget();
    void CleanupDeviceD3D();

    static LRESULT WINAPI WndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);
};

} // namespace qs