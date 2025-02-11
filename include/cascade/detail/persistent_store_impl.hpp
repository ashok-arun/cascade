#pragma once
#include "../persistent_store.hpp"

#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"
#include "delta_store_core.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/persistent/detail/PersistLog.hpp>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_START, group, value);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_START, group, value);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
double PersistentCascadeStore<KT, VT, IK, IV, ST>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}", max_payload_size, duration_sec);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle, max_payload_size, duration_sec);
    debug_leave_func_with_value("{} ops.", ops);
    return ops;
}
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::get(const KT& key, const persistent::version_t& ver, bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return *IV;
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get({})", key);
        return persistent_core->lockless_get(key);
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this, key, requested_version, exact](const VT& v) {
            if(key == v.get_key_ref()) {
                debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                return v;
            } else {
                if(exact) {
                    // return invalid object for EXACT search.
                    debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
                    return *IV;
                } else {
                    // fall back to the slow path.
                    auto versioned_state_ptr = persistent_core.get(requested_version);
                    if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                        debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}", requested_version, key);
                        return versioned_state_ptr->kv_map.at(key);
                    }
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                    return *IV;
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    //  TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}", key, ts_us, stable);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return *IV;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return *IV;
    }

    debug_leave_func();
    return get(key, ver, stable, false);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, const bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return 0ull;
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get_size({})", key);
        return persistent_core->lockless_get_size(key);
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this, key, requested_version, exact](const VT& v) -> uint64_t {
            if(key == v.get_key_ref()) {
                debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                return mutils::bytes_size(v);
            } else {
                if(exact) {
                    // return invalid object for EXACT search.
                    debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
                    return 0ull;
                } else {
                    // fall back to the slow path.
                    auto versioned_state_ptr = persistent_core.get(requested_version);
                    if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                        debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}", requested_version, key);
                        return mutils::bytes_size(versioned_state_ptr->kv_map.at(key));
                    }
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                    return 0ull;
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}", key, ts_us, stable);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return 0;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return 0;
    }

    debug_leave_func();

    return get_size(key, ver, stable);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}.", prefix);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    // TODO: verify consistency ?
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    debug_enter_func_with_args("prefix={}, ver=0x{:x}, stable={}", prefix, ver, stable);

    persistent::version_t requested_version = ver;

    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return {};
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_list_prefix({})", prefix);
        return persistent_core->lockless_list_keys(prefix);
    } else {
        std::vector<KT> keys;
        persistent_core.get(requested_version, [&keys, &prefix](const DeltaCascadeStoreCore<KT, VT, IK, IV>& pers_core) {
            for(const auto& kv : pers_core.kv_map) {
                if(get_pathname<KT>(kv.first).find(prefix) == 0) {
                    keys.push_back(kv.first);
                }
            }
        });
        return keys;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("ts_us={}", ts_us);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return {};
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return {};
    }

    return list_keys(prefix, ver, stable);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#endif
    if(this->internal_ordered_put(value) == false) {
        version_and_timestamp = {persistent::INVALID_VERSION, 0};
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#endif

    this->internal_ordered_put(value);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
#endif

#ifdef ENABLE_EVALUATION
    // avoid unused variable warning.
    if constexpr(!std::is_base_of<IHasMessageID, VT>::value) {
        version_and_timestamp = version_and_timestamp;
    }
#endif
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::internal_ordered_put(const VT& value) {
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if(this->persistent_core->ordered_put(value, this->persistent_core.getLatestVersion()) == false) {
        // verification failed. S we return invalid versions.
        debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
        return false;
    }
    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,
                group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr);
    }
    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    auto value = create_null_object_cb<KT, VT, IK, IV>(key);
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if(this->persistent_core->ordered_remove(value, this->persistent_core.getLatestVersion())) {
        if(cascade_watcher_ptr) {
            (*cascade_watcher_ptr)(
                    // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                    this->subgroup_index,
                    group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                    group->get_rpc_caller_id(),
                    key, value, cascade_context_ptr);
        }
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    debug_leave_func();

    return this->persistent_core->ordered_get(key);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    debug_leave_func();

    return this->persistent_core->ordered_get_size(key);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<PersistentCascadeStore<KT, VT, IK, IV, ST>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr, true);
    }

    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for(auto r : replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}", filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}

#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_list_keys(const std::string& prefix) {
    debug_enter_func();

    debug_leave_func();

    return this->persistent_core->ordered_list_keys(prefix);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<PersistentCascadeStore<KT, VT, IK, IV, ST>> PersistentCascadeStore<KT, VT, IK, IV, ST>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    auto persistent_core_ptr = mutils::from_bytes<persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>>(dsm, buf);
    auto persistent_cascade_store_ptr = std::make_unique<PersistentCascadeStore>(std::move(*persistent_core_ptr),
                                                                                 dsm->registered<CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>>()) : nullptr,
                                                                                 dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
    return persistent_cascade_store_ptr;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore(
        persistent::PersistentRegistry* pr,
        CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : persistent_core([]() {
                                   return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
                               },
                                               nullptr, pr),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore(
        persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&&
                _persistent_core,
        CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : persistent_core(std::move(_persistent_core)),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore() : persistent_core(
        []() {
            return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
        },
        nullptr,
        nullptr),
                                                                       cascade_watcher_ptr(nullptr),
                                                                       cascade_context_ptr(nullptr) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::~PersistentCascadeStore() {}

}  // namespace cascade
}  // namespace derecho
