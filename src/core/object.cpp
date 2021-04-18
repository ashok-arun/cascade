#include <cascade/object.hpp>

namespace derecho {
namespace cascade {

uint64_t ObjectWithUInt64Key::IK = INVALID_UINT64_OBJECT_KEY;
ObjectWithUInt64Key ObjectWithUInt64Key::IV;
std::string ObjectWithStringKey::IK;
ObjectWithStringKey ObjectWithStringKey::IV;
// META
std::string ObjectPoolMetadata::IK;
ObjectPoolMetadata ObjectPoolMetadata::IV;

Blob::Blob(const char* const b, const decltype(size) s) :
    bytes(nullptr), size(0), is_temporary(false) {
    if(s > 0) {
        bytes = new char[s];
        if (b != nullptr) {
            memcpy(bytes, b, s);
        } else {
            bzero(bytes, s);
        }
        size = s;
    }
}

Blob::Blob(char* b, const decltype(size) s, bool temporary) :
    bytes(b), size(s), is_temporary(temporary) {
    if ( (size>0) && (is_temporary==false)) {
        bytes = new char[s];
        if (b != nullptr) {
            memcpy(bytes, b, s);
        } else {
            bzero(bytes, s);
        }
    }
    // exclude illegal argument combination like (0x982374,0,false)
    if (size == 0) {
        bytes = nullptr;
    }
}

Blob::Blob(const Blob& other) :
    bytes(nullptr), size(0) {
    if(other.size > 0) {
        bytes = new char[other.size];
        memcpy(bytes, other.bytes, other.size);
        size = other.size;
    }
}

Blob::Blob(Blob&& other) : 
    bytes(other.bytes), size(other.size) {
    other.bytes = nullptr;
    other.size = 0;
}

Blob::Blob() : bytes(nullptr), size(0) {}

Blob::~Blob() {
    if(bytes && !is_temporary) {
        delete [] bytes;
    }
}

Blob& Blob::operator=(Blob&& other) {
    char* swp_bytes = other.bytes;
    std::size_t swp_size = other.size;
    other.bytes = bytes;
    other.size = size;
    bytes = swp_bytes;
    size = swp_size;
    return *this;
}

Blob& Blob::operator=(const Blob& other) {
    if(bytes != nullptr) {
        delete bytes;
    }
    size = other.size;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, other.bytes, size);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Blob::to_bytes(char* v) const {
    ((std::size_t*)(v))[0] = size;
    if(size > 0) {
        memcpy(v + sizeof(size), bytes, size);
    }
    return size + sizeof(size);
}

std::size_t Blob::bytes_size() const {
    return size + sizeof(size);
}

void Blob::post_object(const std::function<void(char const* const, std::size_t)>& f) const {
    f((char*)&size, sizeof(size));
    f(bytes, size);
}

mutils::context_ptr<Blob> Blob::from_bytes_noalloc(mutils::DeserializationManager* ctx, const char* const v) {
    return mutils::context_ptr<Blob>{new Blob(const_cast<char*>(v) + sizeof(std::size_t), ((std::size_t*)(v))[0], true)};
}

mutils::context_ptr<Blob> Blob::from_bytes_noalloc_const(mutils::DeserializationManager* ctx, const char* const v) {
    return mutils::context_ptr<Blob>{new Blob(const_cast<char*>(v) + sizeof(std::size_t), ((std::size_t*)(v))[0], true)};
}

std::unique_ptr<Blob> Blob::from_bytes(mutils::DeserializationManager*, const char* const v) {
    return std::make_unique<Blob>(v + sizeof(std::size_t), ((std::size_t*)(v))[0]);
}

/*
bool ObjectWithUInt64Key::operator==(const ObjectWithUInt64Key& other) {
    return (this->key == other.key) && (this->version == other.version);
}
*/

bool ObjectWithUInt64Key::is_valid() const {
    return (key != INVALID_UINT64_OBJECT_KEY);
}

// constructor 0 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const Blob& _blob) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_blob) {}

// constructor 0.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const Blob& _blob) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const char* const _b,
                                         const std::size_t _s) :
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}

// constructor 1.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const char* const _b,
                                         const std::size_t _s) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key),
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(ObjectWithUInt64Key&& other) :
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const ObjectWithUInt64Key& other) :
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithUInt64Key::ObjectWithUInt64Key() :
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(INVALID_UINT64_OBJECT_KEY) {}

const uint64_t& ObjectWithUInt64Key::get_key_ref() const {
    return this->key;
}

bool ObjectWithUInt64Key::is_null() const {
    return (this->blob.size == 0);
}

void ObjectWithUInt64Key::set_version(persistent::version_t ver) const {
    this->version = ver;
}

persistent::version_t ObjectWithUInt64Key::get_version() const {
    return this->version;
}

void ObjectWithUInt64Key::set_timestamp(uint64_t ts_us) const {
    this->timestamp_us = ts_us;
}

uint64_t ObjectWithUInt64Key::get_timestamp() const {
    return this->timestamp_us;
}

void ObjectWithUInt64Key::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

bool ObjectWithUInt64Key::verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    // NOTICE: We provide the default behaviour of verify_previous_version as a demonstration. Please change the
    // following code or implementing your own Object Types with a verify_previous_version implementation to customize
    // it. The default behavior is self-explanatory and can be disabled by setting corresponding object previous versions to
    // INVALID_VERSION.

    return ((this->previous_version == persistent::INVALID_VERSION)?true:(this->previous_version >= prev_ver)) &&
           ((this->previous_version_by_key == persistent::INVALID_VERSION)?true:(this->previous_version_by_key >= prev_ver_by_key));
}

template <>
ObjectWithUInt64Key create_null_object_cb<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>(const uint64_t& key) {
    return ObjectWithUInt64Key(key,Blob{});
}

/*
bool ObjectWithStringKey::operator==(const ObjectWithStringKey& other) {
    return (this->key == other.key) && (this->version == other.version);
}
*/

bool ObjectWithStringKey::is_valid() const {
    return !key.empty();
}

// constructor 0 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key, 
                                         const Blob& _blob) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_blob) {}
// constructor 0.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const Blob& _blob) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key,
                                         const char* const _b, 
                                         const std::size_t _s) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}
// constructor 1.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const char* const _b,
                                         const std::size_t _s) : 
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithStringKey::ObjectWithStringKey(ObjectWithStringKey&& other) : 
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const ObjectWithStringKey& other) : 
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithStringKey::ObjectWithStringKey() : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key() {}

const std::string& ObjectWithStringKey::get_key_ref() const {
    return this->key;
}

bool ObjectWithStringKey::is_null() const {
    return (this->blob.size == 0);
}

void ObjectWithStringKey::set_version(persistent::version_t ver) const {
    this->version = ver;
}

persistent::version_t ObjectWithStringKey::get_version() const {
    return this->version;
}

void ObjectWithStringKey::set_timestamp(uint64_t ts_us) const {
    this->timestamp_us = ts_us;
}

uint64_t ObjectWithStringKey::get_timestamp() const {
    return this->timestamp_us;
}

void ObjectWithStringKey::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

bool ObjectWithStringKey::verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    // NOTICE: We provide the default behaviour of verify_previous_version as a demonstration. Please change the
    // following code or implementing your own Object Types with a verify_previous_version implementation to customize
    // it. The default behavior is self-explanatory and can be disabled by setting corresponding object previous versions to
    // INVALID_VERSION.

    return ((this->previous_version == persistent::INVALID_VERSION)?true:(this->previous_version >= prev_ver)) &&
           ((this->previous_version_by_key == persistent::INVALID_VERSION)?true:(this->previous_version_by_key >= prev_ver_by_key));
}

template <>
ObjectWithStringKey create_null_object_cb<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>(const std::string& key) {
    return ObjectWithStringKey(key,Blob{});
}

// META
bool ObjectPoolMetadata::operator==(const ObjectPoolMetadata& other) {
    return (this->object_pool_id == other.object_pool_id) && (this->subgroup_type == other.subgroup_type)
    && (this->subgroup_index == other.subgroup_index) && (this->sharding_policy == other.sharding_policy)
    && (this->objects_locations == other.objects_locations) && (this->version == other.version)
    && (this->timestamp_us == other.timestamp_us) &&(this->deleted == other.deleted);
}

void ObjectPoolMetadata::operator=(const ObjectPoolMetadata& other) {
    this->object_pool_id = other.object_pool_id;
    this->subgroup_type = other.subgroup_type;
    this->subgroup_index = other.subgroup_index;
    this->sharding_policy = other.sharding_policy;
    this->objects_locations = other.objects_locations;
    this->version = other.version;
    this->timestamp_us = other.timestamp_us;
    this->deleted = other.deleted;
}

bool ObjectPoolMetadata::is_valid() const {
    return ((!object_pool_id.empty()) && (0 == this->deleted));
}

// constructor 0 : copy constructor
ObjectPoolMetadata::ObjectPoolMetadata(const std::string& _object_pool_id, 
                        const std::string& _subgroup_type, const uint32_t _subgroup_index,
                        const int _sharding_policy): 
    object_pool_id(_object_pool_id),
    subgroup_type(_subgroup_type),
    subgroup_index(_subgroup_index),
    sharding_policy(_sharding_policy),
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    deleted(0) {}

// constructor 0.5 : copy constructor
ObjectPoolMetadata::ObjectPoolMetadata(const std::string& _object_pool_id, 
                        const std::string& _subgroup_type, const uint32_t _subgroup_index,
                        const int _sharding_policy,
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us): 
    object_pool_id(_object_pool_id),
    subgroup_type(_subgroup_type),
    subgroup_index(_subgroup_index),
    sharding_policy(_sharding_policy),
    version(_version),
    timestamp_us(_timestamp_us),
    deleted(0) {}

    
// constructor 1 : copy consotructor
ObjectPoolMetadata::ObjectPoolMetadata(const std::string& _object_pool_id, 
                        const std::string& _subgroup_type, const uint32_t _subgroup_index,
                        const int _sharding_policy,
                        const std::unordered_map<std::string,uint32_t> _objects_locations): 
    object_pool_id(_object_pool_id),
    subgroup_type(_subgroup_type),
    subgroup_index(_subgroup_index),
    sharding_policy(_sharding_policy),
    objects_locations(_objects_locations),
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    deleted(0) {}

// constructor 1.5: copy constructor
ObjectPoolMetadata::ObjectPoolMetadata(const std::string& _object_pool_id, 
                    const std::string& _subgroup_type, const uint32_t _subgroup_index,
                    const int _sharding_policy, 
                    const std::unordered_map<std::string,uint32_t> _objects_locations,
                    const persistent::version_t _version,
                    const uint64_t _timestamp_us):
    object_pool_id(_object_pool_id),
    subgroup_type(_subgroup_type),
    subgroup_index(_subgroup_index),
    sharding_policy(_sharding_policy),
    objects_locations(_objects_locations),
    version(_version),
    timestamp_us(_timestamp_us),
    deleted(0) {}

// constructor 2 : copy consotructor
ObjectPoolMetadata::ObjectPoolMetadata(const std::string& _object_pool_id, 
                        const std::string& _subgroup_type, const uint32_t _subgroup_index,
                        const int _sharding_policy,
                        const std::unordered_map<std::string,uint32_t> _objects_locations,
                        const int _deleted): 
    object_pool_id(_object_pool_id),
    subgroup_type(_subgroup_type),
    subgroup_index(_subgroup_index),
    sharding_policy(_sharding_policy),
    objects_locations(_objects_locations),
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    deleted(_deleted) {}

// constructor 3 : move constructor
ObjectPoolMetadata::ObjectPoolMetadata(ObjectPoolMetadata&& other) : 
    object_pool_id(other.object_pool_id),
    subgroup_type(other.subgroup_type),
    subgroup_index(other.subgroup_index),
    sharding_policy(other.sharding_policy),
    objects_locations(other.objects_locations),
    version(other.version),
    timestamp_us(other.timestamp_us),
    deleted(other.deleted) {}

// constructor 4 : copy constructor
ObjectPoolMetadata::ObjectPoolMetadata(const ObjectPoolMetadata& other) : 
    object_pool_id(other.object_pool_id),
    subgroup_type(other.subgroup_type),
    subgroup_index(other.subgroup_index),
    sharding_policy(other.sharding_policy),
    objects_locations(other.objects_locations),
    deleted(other.deleted) {}

// constructor 5 : default invalid constructor
ObjectPoolMetadata::ObjectPoolMetadata() : 
    object_pool_id(""),
    subgroup_type(""),
    subgroup_index(0),
    sharding_policy(0),
    deleted(0) {}

void ObjectPoolMetadata::set_deleted(){
    this->deleted = 1;
}
const std::string& ObjectPoolMetadata::get_key_ref() const {
    return this->object_pool_id;
}

bool ObjectPoolMetadata::is_null() const {
    return (this->subgroup_type.size() == 0);
}

void ObjectPoolMetadata::set_version(persistent::version_t ver) const {
    this->version = ver;
}

persistent::version_t ObjectPoolMetadata::get_version() const {
    return this->version;
}

void ObjectPoolMetadata::set_timestamp(uint64_t ts_us) const {
    this->timestamp_us = ts_us;
}

uint64_t ObjectPoolMetadata::get_timestamp() const {
    return this->timestamp_us;
}

void ObjectPoolMetadata::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {

}

bool ObjectPoolMetadata::verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    return true;
}

template <>
ObjectPoolMetadata create_null_object_cb<std::string,ObjectPoolMetadata,&ObjectPoolMetadata::IK,&ObjectPoolMetadata::IV>(const std::string& object_pool_id) {
    return ObjectPoolMetadata(object_pool_id,"",0,0);
}


} // namespace cascade
} // namespace derecho

