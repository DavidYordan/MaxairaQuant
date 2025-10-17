cmake_minimum_required(VERSION 3.28)
project(QuantStudio VERSION 0.2.0 DESCRIPTION "High-performance quant terminal" LANGUAGES CXX)

# 生成 compile_commands.json，便于 IDE/静态分析
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 选项
option(QS_SSL_VERIFY "Enable TLS certificate verification" OFF)
option(QS_ENABLE_IPO "Enable LTO/IPO in Release" ON)
option(QS_BUILD_TESTS "Build unit tests" OFF)

# 对 MSVC 进行警告等级与 UTF-8 编译配置
if (MSVC)
  add_compile_options(/W4 /permissive- /Zc:preprocessor /utf-8)
  add_definitions(-DUNICODE -D_UNICODE -DNOMINMAX)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# LTO/IPO（仅 Release）
if(QS_ENABLE_IPO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ipo_supported OUTPUT ipo_msg)
  if(ipo_supported)
    set_property(GLOBAL PROPERTY INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
  else()
    message(WARNING "IPO/LTO not supported: ${ipo_msg}")
  endif()
endif()

# 依赖（vcpkg manifest 模式）
find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS asio beast system)
find_package(imgui CONFIG REQUIRED)
find_package(implot CONFIG REQUIRED)

add_executable(QuantStudio)

target_sources(QuantStudio PRIVATE
  src/main.cpp
  src/app/App.cpp
  src/network/WSClient.cpp
  src/indicator/IndicatorEngine.cpp
  src/data/DataStore.cpp
  src/ui/UIDemoDX11.cpp
)

target_include_directories(QuantStudio PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# 以目标为中心的现代设置
target_compile_features(QuantStudio PRIVATE cxx_std_23)

# 跨编译器警告与 MSVC UTF-8/严格模式
target_compile_options(QuantStudio PRIVATE
  $<$<CXX_COMPILER_ID:MSVC>:/W4;/Zc:preprocessor;/Zc:__cplusplus;/permissive-;/EHsc;/utf-8>
  $<$<CXX_COMPILER_ID:GNU>:-Wall;-Wextra;-Wpedantic>
  $<$<CXX_COMPILER_ID:Clang>:-Wall;-Wextra;-Wpedantic>
)

# 编译定义
target_compile_definitions(QuantStudio PRIVATE
  UNICODE _UNICODE NOMINMAX WIN32_LEAN_AND_MEAN
  _WIN32_WINNT=0x0A00
  $<$<BOOL:${QS_SSL_VERIFY}>:QS_SSL_VERIFY>
)

# 链接库（使用导入目标）
target_link_libraries(QuantStudio PRIVATE
  spdlog::spdlog
  fmt::fmt
  nlohmann_json::nlohmann_json
  yaml-cpp::yaml-cpp
  OpenSSL::SSL
  OpenSSL::Crypto
  Boost::asio
  Boost::beast
  Boost::system
  imgui::imgui
  implot::implot
  d3d11
  dxgi
  d3dcompiler
)

# ------------------------------------------------------
# 自动复制配置文件夹到构建输出目录
# ------------------------------------------------------
add_custom_command(
  TARGET QuantStudio POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_directory
          ${CMAKE_SOURCE_DIR}/config
          $<TARGET_FILE_DIR:QuantStudio>/config
  COMMENT "Copying config directory to build output..."
)

# MSVC 运行库（静态/动态可按需切换；此处示例为静态）
if(MSVC)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
endif()

# 安装规则
include(GNUInstallDirs)
install(TARGETS QuantStudio RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/config/app.yaml DESTINATION ${CMAKE_INSTALL_DATADIR}/QuantStudio)
