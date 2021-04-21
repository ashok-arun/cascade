#include <cascade/data_path_logic_interface.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <opencv2/opencv.hpp>
#include <cppflow/cppflow.h>
#include <torch/script.h>
#include <ANN/ANN.h>		

namespace derecho{
namespace cascade{

#define MY_UUID     "6793c66c-9d92-11eb-9aa9-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO inference DPL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

/* returns [<frame_idx>, <frame_ts>] */
std::tuple<std::string,std::string> parse_key(const std::string& obj_key) {
    std::string delim_bs("/"), delim_us("_");
    std::string frame_key = obj_key.substr(obj_key.rfind(delim_bs) + 1);
    size_t pos = frame_key.find(delim_us);
    return std::make_tuple(frame_key.substr(0,pos),frame_key.substr(pos+1));
}

#define K                    (5)			// number of nearest neighbors
#define DIM		             (128)			// dimension
#define EPS		             (0)			// error bound
#define MAX_PTS		         (5000)		    // maximum number of data points
#define COW_ID_IMAGE_WIDTH   (224)
#define COW_ID_IMAGE_HEIGHT  (224)
#define DPL_CONF_COWID_MODULE       "CASCADE/cow_id_module"
#define DPL_CONF_COWID_KNN          "CASCADE/cow_id_knn"
#define DPL_CONF_COWID_LABEL        "CASCADE/cow_id_label"

class InferenceEngine {
private: 
    torch::jit::script::Module module;
    
    torch::NoGradGuard no_grad;     // ensures that autograd is off
    
    int labels[MAX_PTS];

    // image embedding
    ANNpoint img_emb;

    // near neighbor indices
    ANNidxArray nnIdx;		

    // near neighbor distances
	ANNdistArray dists;	
    
    // search structure				
	ANNkd_tree*	kdTree;					

    at::Tensor to_tensor(const cv::Mat& mat, bool unsqueeze=false, int unsqueeze_dim=0) {
        at::Tensor tensor_image = torch::from_blob(mat.data, {mat.rows,mat.cols,3}, at::kByte);
        if (unsqueeze) {
            tensor_image.unsqueeze_(unsqueeze_dim);
        }
        return tensor_image;
    }

    void load_module(const std::string& module_file) {
        try {
            module = torch::jit::load(module_file);
            module.eval();                          // turn off dropout and other training-time layers/functions
            dbg_default_trace("loaded module: "+module_file);
        } catch (const c10::Error& e) {
            std::cerr << "Error loading model\n";
            std::cerr << e.what_without_backtrace();
            return;
        }
    }

    void load_knn(const std::string& knn_file) {
        img_emb = annAllocPt(DIM);					// allocate image embedding
	    nnIdx = new ANNidx[K];						// allocate near neighbor indices
	    dists = new ANNdist[K];						// allocate near neighbor dists
        std::ifstream trainedKNN(knn_file);
        kdTree = new ANNkd_tree(trainedKNN);        // restore a copy of the old tree
        trainedKNN.close();
        dbg_default_trace("loaded knn: "+knn_file);
    }

    void load_labels(const std::string& label_file) {
        std::ifstream label_fs(label_file);
        if (!label_fs.is_open()) {
            std::cerr << "Could not open the file - '" << label_file << "'" << std::endl;
            return;
        }
        int count = 0;
        int x;
        while (count < MAX_PTS && label_fs >> x) {
            labels[count++] = x;
        }
        label_fs.close();
        dbg_default_trace("loaded label file: "+label_file);
    }
    
public:
    InferenceEngine(const std::string& module_file, const std::string& knn_file, const std::string& label_file) {
        load_module(module_file);
        load_knn(knn_file);
        load_labels(label_file);
    }

    uint32_t infer(const cv::Mat& mat) {
        // convert to tensor
        at::Tensor tensor = to_tensor(mat);
        tensor = tensor.toType(c10::kFloat).div(255);
        tensor = tensor.permute({2, 0, 1});
        //add batch dim (an inplace operation just like in pytorch)
        tensor.unsqueeze_(0);
        std::vector<torch::jit::IValue> inputs;
        inputs.push_back(tensor);
        dbg_default_trace("image is loaded");
        // execute model and package output as tensor
        at::Tensor output = module.forward(inputs).toTensor();
        for (int i = 0; i < DIM; i++) {
            img_emb[i] = output[0][i].item().to<double>();
        }
        kdTree->annkSearch(img_emb, K, nnIdx, dists, EPS);							
        // use the most nearest neighbor, could find mode of K nearest neighbor
        return static_cast<uint32_t>(labels[nnIdx[0]]);
    } 

    ~InferenceEngine() {
        delete[] nnIdx;
        delete[] dists;
        delete kdTree;
        annClose();
    }
};

void infer_cow_id(uint32_t* cow_id, void* img_buf, size_t img_size) {
    InferenceEngine cow_id_ie(derecho::getConfString(DPL_CONF_COWID_MODULE),derecho::getConfString(DPL_CONF_COWID_KNN),derecho::getConfString(DPL_CONF_COWID_LABEL));
    std::vector<unsigned char> out_buf(img_size);
    std::memcpy(static_cast<void*>(out_buf.data()),static_cast<const void*>(img_buf),img_size);
    cv::Mat mat(240,352,CV_32FC3,out_buf.data());
    // resize to desired dimension matching with the model
    cv::resize(mat, mat, cv::Size(COW_ID_IMAGE_WIDTH,COW_ID_IMAGE_HEIGHT));
    *cow_id = cow_id_ie.infer(mat);
}

#define BCS_IMAGE_HEIGHT           (300)
#define BCS_IMAGE_WIDTH            (300)
#define BCS_TENSOR_BUFFER_SIZE     (BCS_IMAGE_HEIGHT*BCS_IMAGE_WIDTH*3)
#define DPL_CONF_INFER_BCS_MODEL   "CASCADE/bcs_model"

void infer_bcs(float* bcs, void* img_buf, size_t img_size) {
    /* step 1: load the model */ 
    cppflow::model model(derecho::getConfString(DPL_CONF_INFER_BCS_MODEL));
    
    /* step 2: Load the image & convert to tensor */
    std::vector<unsigned char> out_buf(img_size);
    std::memcpy(static_cast<void*>(out_buf.data()),static_cast<const void*>(img_buf),img_size);
    cv::Mat mat(240,352,CV_32FC3,out_buf.data());
    // resize to desired dimension matching with the model & convert to tensor
    cv::resize(mat, mat, cv::Size(BCS_IMAGE_WIDTH,BCS_IMAGE_HEIGHT));
    std::vector<float> bcs_tensor_buf(BCS_TENSOR_BUFFER_SIZE);
    std::memcpy(static_cast<void*>(bcs_tensor_buf.data()), static_cast<const void*>(mat.data),BCS_TENSOR_BUFFER_SIZE*sizeof(float));
    cppflow::tensor input_tensor(bcs_tensor_buf, {BCS_IMAGE_WIDTH,BCS_IMAGE_HEIGHT,3});
    input_tensor = cppflow::expand_dims(input_tensor, 0);
    
    /* step 3: Predict */
    cppflow::tensor output = model({{"serving_default_conv2d_5_input:0", input_tensor}},{"StatefulPartitionedCall:0"})[0];
    float prediction = output.get_data<float>()[0];
    *bcs = prediction;
    std::cout << "prediction is: " << std::to_string(prediction) << std::endl;
}

class DairyFarmInferOCDPO: public OffCriticalDataPathObserver {
private:
    mutable std::mutex p2p_send_mutex;

    virtual void operator () (const std::string& key_string,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // TODO: do inference and put it to the storage object pool specified by outputs.
        const VolatileCascadeStoreWithStringKey::ObjectType *vcss_value = reinterpret_cast<const VolatileCascadeStoreWithStringKey::ObjectType *>(value_ptr);
        uint32_t cow_id;
        std::cout << "\033[1;31m"
              << "Beginning in infer"<< key_string
              << "\033[0m" << std::endl;
        float bcs; 
        std::thread cow_id_inference(infer_cow_id, &cow_id, vcss_value->blob.bytes, vcss_value->blob.size);
        std::thread bcs_inference(infer_bcs, &bcs, vcss_value->blob.bytes, vcss_value->blob.size);

        std::cout << "\033[1;31m"
              << "finished first join"
              << "\033[0m" << std::endl;
        cow_id_inference.join();
        bcs_inference.join();
        dbg_default_debug( "\n\n finished infer"+cow_id);
        
        // put the result to next tier
        std::string delim("/");
        std::tuple<std::string, std::string> idx_ts = std::move(parse_key(key_string));
        std::string obj_value = std::to_string(bcs) + "_" + std::get<1>(idx_ts);
        std::cout << "\033[1;31m"
              << "Obj_value"<< obj_value
              << "\033[0m" << std::endl;
        for (auto iter = outputs.begin(); iter != outputs.end(); ++iter) {
            std::string obj_key = iter->first + delim + std::to_string(cow_id);
            PersistentCascadeStoreWithStringKey::ObjectType obj(obj_key,obj_value.c_str(),obj_value.size());
            std::lock_guard<std::mutex> lock(p2p_send_mutex);
            auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeMetadataWithStringKey, VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>*>(ctxt);
            // if true, use trigger put; otherwise, use normal put
            if (iter->second) {
                auto result = typed_ctxt->get_service_client_ref().template trigger_put<PersistentCascadeStoreWithStringKey>(obj);
                // for (auto& reply_future:result.get()) {
                //     dbg_default_debug("node({}) fulfilled the reply futures",reply_future.first);
                // }
            } 
            else {
                auto result = typed_ctxt->get_service_client_ref().template put<PersistentCascadeStoreWithStringKey>(obj);
                for (auto& reply_future:result.get()) {
                    auto reply = reply_future.second.get();
                    dbg_default_debug("node({}) replied with version:({:x},{}us)",reply_future.first,std::get<0>(reply),std::get<1>(reply));
                }
            }
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmInferOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmInferOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DairyFarmInferOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer() {
    return DairyFarmInferOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho