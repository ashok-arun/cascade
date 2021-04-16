#include <cascade/data_path_logic_interface.hpp>
#include <iostream>
#include <vector>
#include <opencv2/opencv.hpp>
#include <cppflow/cppflow.h>

namespace derecho{
namespace cascade{

#define MY_UUID     "22b86c6e-9d92-11eb-81d0-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO: Filter DPL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

#define PHOTO_WIDTH            (352)
#define PHOTO_HEIGHT           (240)
#define TENSOR_BUFFER_SIZE     (PHOTO_WIDTH*PHOTO_HEIGHT*3)
#define DPL_CONF_FILTER_MODEL  "CASCADE/filter_model"

class DairyFarmFilterOCDPO: public OffCriticalDataPathObserver {
    std::mutex p2p_send_mutex;

    virtual void operator () (const std::string& key_string,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // TODO: test if there is a cow in the incoming frame.
        /* step 1: load the model */ 
        static thread_local cppflow::model model(derecho::getConfString(DPL_CONF_FILTER_MODEL));
        
        /* step 2: Load the image & convert to tensor */
        const VolatileCascadeStoreWithStringKey::ObjectType *tcss_value = reinterpret_cast<const VolatileCascadeStoreWithStringKey::ObjectType *>(value_ptr);
        std::vector<float> tensor_buf(TENSOR_BUFFER_SIZE);
        std::memcpy(static_cast<void*>(tensor_buf.data()),static_cast<const void*>(tcss_value->blob.bytes), tcss_value->blob.size);
        cppflow::tensor input_tensor(tensor_buf, {1,PHOTO_WIDTH,PHOTO_HEIGHT,3});
        // input_tensor = cppflow::expand_dims(input_tensor, 0);
        
        /* step 3: Predict */
        cppflow::tensor output = model({{"serving_default_conv2d_3_input:0", input_tensor}},{"StatefulPartitionedCall:0"})[0];
        
        /* step 4: Send intermediate results to the next tier if image frame is meaningful */
        // prediction < 0.35 indicates strong possibility that the image frame captures full contour of the cow
        float prediction = output.get_data<float>()[0];
        std::cout << "prediction: " << prediction << std::endl;
        if (prediction < 0.35) {
            std::string delim("/");
            std::string frame_idx = key_string.substr(key_string.rfind(delim) + 1);
            for (auto iter = outputs.begin(); iter != outputs.end(); ++iter) {
                std::string obj_key = iter->first + delim + frame_idx;
                VolatileCascadeStoreWithStringKey::ObjectType obj(obj_key,tcss_value->blob.bytes,tcss_value->blob.size);
                std::lock_guard<std::mutex> lock(p2p_send_mutex);
                auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>*>(ctxt);
                // if true, use trigger put; otherwise, use normal put
                if (iter->second) {
                    auto result = typed_ctxt->get_service_client_ref().template trigger_put<VolatileCascadeStoreWithStringKey>(obj);
                    // for (auto& reply_future:result.get()) {
                    //     dbg_default_debug("node({}) fulfilled the reply futures",reply_future.first);
                    // }
                } 
                else {
                    auto result = typed_ctxt->get_service_client_ref().template put<VolatileCascadeStoreWithStringKey>(obj);
                    for (auto& reply_future:result.get()) {
                        auto reply = reply_future.second.get();
                        dbg_default_debug("node({}) replied with version:({:x},{}us)",reply_future.first,std::get<0>(reply),std::get<1>(reply));
                    }
                }
            }
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmFilterOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmFilterOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DairyFarmFilterOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer() {
    return DairyFarmFilterOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
