cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

CHECK_INCLUDE_FILES("mxnet-cpp/MxNetCpp.h" HAS_MXNET_CPP)

add_library(console_printer_dpl SHARED console_printer_dpl.cpp)
target_include_directories(console_printer_dpl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
add_custom_command(TARGET console_printer_dpl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/run.sh 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/run.sh 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/run.sh 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/run.sh 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/run.sh 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/dfgs.json 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/dfgs.json 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/dfgs.json 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/dfgs.json 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/dfgs.json 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/dpl_dlls.cfg 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/dpl_dlls.cfg 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/dpl_dlls.cfg 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/dpl_dlls.cfg 
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/dpl_dlls.cfg 
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/run.sh.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/dpl_dlls.cfg.tmp
    COMMENT "prepare console_printer configuration"
)

