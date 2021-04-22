cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

find_library(TENSORFLOW_LIB tensorflow)

if (TENSORFLOW_LIB)
    add_library(filter_dpl SHARED filter_dpl.cpp)
    target_link_libraries(filter_dpl cascade ${OpenCV_LIBS} ${TENSORFLOW_LIB})
    target_include_directories(filter_dpl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
        $<BUILD_INTERFACE:${TENSORFLOW_LIB_INCLUDE_DIRS}>
    )
    
    add_custom_command(TARGET filter_dpl POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/video_receive.py
        ${CMAKE_CURRENT_BINARY_DIR}/video_receive.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz
        ${CMAKE_CURRENT_BINARY_DIR}/filter-model.tar.gz
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/extract_model.sh
        ${CMAKE_CURRENT_BINARY_DIR}/extract_model.sh
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/dpl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/dpl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/dpl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/dpl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/dpl_dlls.cfg 
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/run.sh.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/dfgs.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/dpl_dlls.cfg.tmp
        COMMENT "prepare dairy farm demo configuration"
    )

    find_package(Torch)
    if (TORCH_FOUND)
        add_library(infer_dpl SHARED infer_dpl.cpp)
        target_link_libraries(infer_dpl cascade ${TENSORFLOW_LIB} ${TORCH_LIBRARIES} ${OpenCV_LIBS})
        target_include_directories(infer_dpl PRIVATE
            $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
            $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
            $<BUILD_INTERFACE:${TORCH_INCLUDE_DIRS}>
            $<BUILD_INTERFACE:${TENSORFLOW_LIB_INCLUDE_DIRS}>
            $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
        )
        target_link_libraries(infer_dpl cascade ${TENSORFLOW_LIB} ${TORCH_LIBRARIES} ${OpenCV_LIBS} ANN)

        add_custom_command(TARGET infer_dpl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/bcs-inference-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/bcs-inference-model.tar.gz
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model.tar.gz
        )
    endif()
endif()

#TODO: enable cnn_classifier dpl after data path logic manager is finished.
if (HAS_MXNET_CPP AND OPENCV_CORE_FOUND)

   add_executable(farm_image_client farm_image_client.cpp)
   target_include_directories(farm_image_client PRIVATE
       $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
       $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
       $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
       $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
   )
   target_link_libraries(farm_image_client cascade mxnet ${OpenCV_LIBS})
   add_custom_command(TARGET farm_image_client POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow_frame1.jpg
        ${CMAKE_CURRENT_BINARY_DIR}/cow_frame1.jpg
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow_frame2.jpg
        ${CMAKE_CURRENT_BINARY_DIR}/cow_frame2.jpg
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow_frame3.jpg
        ${CMAKE_CURRENT_BINARY_DIR}/cow_frame3.jpg
   )
endif()