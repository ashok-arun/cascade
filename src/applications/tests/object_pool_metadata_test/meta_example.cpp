#include <iostream>
#include <vector>
#include <memory>
#include <derecho/core/derecho.hpp>
#include <derecho/utils/logger.hpp>
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include "cascade/service_types.hpp"
#include "cascade/service.hpp"

using namespace derecho::cascade;
using derecho::ExternalClientCaller;
using MCS = VolatileCascadeStore<std::string,ObjectPoolMetadata,&ObjectPoolMetadata::IK,&ObjectPoolMetadata::IV>;
using VCS = VolatileCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;
using PCS = PersistentCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV,ST_FILE>;
using TCS = TriggerCascadeNoStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;
using ServiceClientAPI = ServiceClient<MCS, VCS, PCS,TCS>;

#define check_put_and_remove_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)\
                  << ",ts_us:" << std::get<1>(reply) << std::endl;\
    }

#define check_get_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl;\
    }

// test and compare
static void test_compare_objectpool_placement(ServiceClientAPI& capi, derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>>& result,
                                        std::string object_pool_id, ObjectPoolMetadata& expected_metadata, std::string test_name){
    check_put_and_remove_result(result);
    ObjectPoolMetadata placed_metadata = capi.find_object_pool(object_pool_id);
    if (placed_metadata == expected_metadata){ 
        std::cout <<"--"<< test_name <<" passed. \n";
    }else{
        std::cout << "\033[1;31m" << "--" << test_name << " NOT passed. \n" << "\033[0m" << std::endl;;
    }
}

// test remove and compare
static void test_remove_objectpool(ServiceClientAPI& capi, derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>>& result,
                                        std::string object_pool_id,  std::string test_name){
    check_put_and_remove_result(result);
    ObjectPoolMetadata placed_metadata = capi.find_object_pool(object_pool_id);
    if (placed_metadata.is_valid()){ 
        std::cout <<"--"<< test_name <<" removed. \n";
    }else{
        std::cout << "\033[1;31m" << "--" << test_name << " NOT removed. \n" << "\033[0m" << std::endl;;
    }
}


// CREATE object pool metadata
static void create_object_pool_test(ServiceClientAPI& capi){
    // Test 1
    uint32_t subgroup_index1 = 0;
    std::string o_p_id1 = "farm1";
    std::string subgroup_type1 = "VCSS";
    ObjectPoolMetadata obj_pool_metadata1(o_p_id1, subgroup_type1,subgroup_index1); 
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result1 = std::move( capi.create_object_pool(o_p_id1, obj_pool_metadata1));
    test_compare_objectpool_placement(capi, result1, o_p_id1, obj_pool_metadata1, "Create object pool metadata Test [1]");

    // Test 2
    uint32_t subgroup_index2 = 0;
    std::string o_p_id2 = "farm1/camera1";
    std::string subgroup_type2 = "VCSS";
    int sharding_policy2 = 2;
    ObjectPoolMetadata obj_pool_metadata2(o_p_id2, subgroup_type2, subgroup_index2, sharding_policy2); 
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result2 = std::move( capi.create_object_pool(o_p_id2, obj_pool_metadata2));
    test_compare_objectpool_placement(capi,  result2, o_p_id2, obj_pool_metadata2, "Create object pool metadata Test [2]");

    // Test 3
    uint32_t subgroup_index3 = 0;
    std::string o_p_id3 = "farm1/camera1/cowIdentification";
    std::string subgroup_type3 = "PCSS";
    int sharding_policy3 = 1;
    std::unordered_map<std::string, uint32_t>   objects_locations3 = {
        {"parameter1",0},
        {"parameter2",3},
        {"cow1_frame",1}   
    };
    ObjectPoolMetadata obj_pool_metadata3(o_p_id3, subgroup_type3, subgroup_index3, sharding_policy3, objects_locations3); 
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result3 = std::move( capi.create_object_pool(o_p_id3, obj_pool_metadata3));
    test_compare_objectpool_placement(capi,  result3, o_p_id3, obj_pool_metadata3, "Create object pool metadata Test [3]");

    // Test 4: reput the same object pool, should not replace the original one
    uint32_t subgroup_index1_r = 1;
    std::string subgroup_type1_r = "PCSS";
    int sharding_policy1_r = 2;
    ObjectPoolMetadata obj_pool_metadata1_r(o_p_id1, subgroup_type1_r,subgroup_index1_r,sharding_policy1_r); 
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result1_r = std::move( capi.create_object_pool(o_p_id1, obj_pool_metadata1_r));
    test_compare_objectpool_placement(capi, result1_r, o_p_id1, obj_pool_metadata1, "Create object pool metadata Test [4]");

}

// REMOVE object pool metadata
static void remove_object_pool_test(ServiceClientAPI& capi){
    // Test 1
    std::string o_p_id1 = "farm1";
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>>  remove_result1 =  capi.remove_object_pool(o_p_id1);
    test_remove_objectpool(capi, remove_result1, o_p_id1,  "Remove object pool metadata farm1 Test [1]");

    // Test 2
    std::string o_p_id2 = "farm1/camera1";
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove_result2 = std::move( capi.remove_object_pool(o_p_id2));
    test_remove_objectpool(capi,  remove_result2, o_p_id2,  "Remove object pool metadata farm1/camera1 Test [2]");

    // Test 3
    std::string o_p_id3 = "farm1/camera1/cowIdentification"; 
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove_result3 = std::move( capi.remove_object_pool(o_p_id3));
    test_remove_objectpool(capi,  remove_result3, o_p_id3,  "Remove object pool metadata farm1/camera1/cowIdentification Test [3]");
}


// PUT to object pool
static void put_to_objectPool_test(ServiceClientAPI& capi){
    uint32_t subgroup_index = 0, shard_index = 0;
    ObjectWithStringKey obj;
    persistent::version_t pver = persistent::INVALID_VERSION;
    persistent::version_t pver_bk = persistent::INVALID_VERSION;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.key = "farm1/camera1/img1";
    std::string value = "farm cow photo1";
    obj.blob = Blob(value.c_str(),value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = capi.template put<VCS>(obj, subgroup_index, shard_index);
    check_put_and_remove_result(result);
}

// GET from object pool
static void get_from_objectPool_test(ServiceClientAPI& capi){
    uint32_t subgroup_index = 0, shard_index = 0;
    persistent::version_t ver = persistent::INVALID_VERSION;
    std::string key = "farm1/camera1/img1";
    derecho::rpc::QueryResults<const typename VCS::ObjectType> result = capi.template get<VCS>(
                key,ver,subgroup_index,shard_index, true /*use object pool get*/);
    check_get_result(result);
}

int main(int argc, char** argv) {
    /** initialize the parameters */
    derecho::Conf::initialize(argc,argv);
    ServiceClientAPI capi(true);
    create_object_pool_test(capi);
    remove_object_pool_test(capi);
    put_to_objectPool_test(capi);
    get_from_objectPool_test(capi);
    return 0;
}