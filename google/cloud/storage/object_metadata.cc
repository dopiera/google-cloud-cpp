// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/storage/object_metadata.h"
#include "google/cloud/storage/internal/format_rfc3339.h"
#include "google/cloud/storage/internal/metadata_parser.h"
#include "google/cloud/storage/internal/nljson.h"
#include "google/cloud/storage/internal/object_acl_requests.h"

namespace google {
namespace cloud {
namespace storage {
inline namespace STORAGE_CLIENT_NS {

bool ObjectMetadata::operator==(ObjectMetadata const& rhs) const {
  return static_cast<internal::CommonMetadata<ObjectMetadata> const&>(*this) ==
             rhs and
         acl_ == rhs.acl_ && bucket_ == rhs.bucket_ &&
         cache_control_ == rhs.cache_control_ &&
         component_count_ == rhs.component_count_ &&
         content_disposition_ == rhs.content_disposition_ &&
         content_encoding_ == rhs.content_encoding_ &&
         content_language_ == rhs.content_language_ &&
         content_type_ == rhs.content_type_ && crc32c_ == rhs.crc32c_ &&
         customer_encryption_ == customer_encryption_ &&
         event_based_hold_ == rhs.event_based_hold_ &&
         generation_ == rhs.generation_ && kms_key_name_ == rhs.kms_key_name_ &&
         md5_hash_ == rhs.md5_hash_ && media_link_ == rhs.media_link_ &&
         metadata_ == rhs.metadata_ &&
         retention_expiration_time_ == rhs.retention_expiration_time_ &&
         temporary_hold_ == rhs.temporary_hold_ &&
         time_deleted_ == rhs.time_deleted_ &&
         time_storage_class_updated_ == rhs.time_storage_class_updated_ &&
         size_ == rhs.size_;
}

std::ostream& operator<<(std::ostream& os, ObjectMetadata const& rhs) {
  os << "ObjectMetadata={name=" << rhs.name() << ", acl=[";
  char const* sep = "";
  for (auto const& acl : rhs.acl()) {
    os << sep << acl;
    sep = ", ";
  }
  os << "], bucket=" << rhs.bucket()
     << ", cache_control=" << rhs.cache_control()
     << ", component_count=" << rhs.component_count()
     << ", content_disposition=" << rhs.content_disposition()
     << ", content_encoding=" << rhs.content_encoding()
     << ", content_language=" << rhs.content_language()
     << ", content_type=" << rhs.content_type() << ", crc32c=" << rhs.crc32c();

  if (rhs.has_customer_encryption()) {
    os << ", customer_encryption.encryption_algorithm="
       << rhs.customer_encryption().encryption_algorithm
       << ", customer_encryption.key_sha256="
       << rhs.customer_encryption().key_sha256;
  }

  os << ", etag=" << rhs.etag() << ", event_based_hold=" << std::boolalpha
     << rhs.event_based_hold() << ", generation=" << rhs.generation()
     << ", id=" << rhs.id() << ", kind=" << rhs.kind()
     << ", kms_key_name=" << rhs.kms_key_name()
     << ", md5_hash=" << rhs.md5_hash() << ", media_link=" << rhs.media_link();
  sep = "metadata.";
  for (auto const& kv : rhs.metadata_) {
    os << sep << kv.first << "=" << kv.second;
    sep = ", metadata.";
  }
  os << ", metageneration=" << rhs.metageneration() << ", name=" << rhs.name();

  if (rhs.has_owner()) {
    os << ", owner.entity=" << rhs.owner().entity
       << ", owner.entity_id=" << rhs.owner().entity_id;
  }

  os << ", retention_expiration_time="
     << internal::FormatRfc3339(rhs.retention_expiration_time())
     << ", self_link=" << rhs.self_link() << ", size=" << rhs.size()
     << ", storage_class=" << rhs.storage_class()
     << ", temporary_hold=" << std::boolalpha << rhs.temporary_hold()
     << ", time_created=" << rhs.time_created().time_since_epoch().count()
     << ", time_deleted=" << rhs.time_deleted().time_since_epoch().count()
     << ", time_storage_class_updated="
     << rhs.time_storage_class_updated().time_since_epoch().count()
     << ", updated=" << rhs.updated().time_since_epoch().count() << "}";
  return os;
}

std::string ObjectMetadataPatchBuilder::BuildPatch() const {
  internal::PatchBuilder tmp = impl_;
  if (metadata_subpatch_dirty_) {
    if (metadata_subpatch_.empty()) {
      tmp.RemoveField("metadata");
    } else {
      tmp.AddSubPatch("metadata", metadata_subpatch_);
    }
  }
  return tmp.ToString();
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetAcl(
    std::vector<ObjectAccessControl> const& v) {
  if (v.empty()) {
    return ResetAcl();
  }
  std::vector<internal::nl::json> array;
  array.reserve(v.size());
  for (auto const& a : v) {
    array.emplace_back(internal::nl::json{
        {"entity", a.entity()},
        {"role", a.role()},
    });
  }
  impl_.SetArrayField("acl", array);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetAcl() {
  impl_.RemoveField("acl");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetCacheControl(
    std::string const& v) {
  if (v.empty()) {
    return ResetCacheControl();
  }
  impl_.SetStringField("cacheControl", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetCacheControl() {
  impl_.RemoveField("cacheControl");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetContentDisposition(
    std::string const& v) {
  if (v.empty()) {
    return ResetContentDisposition();
  }
  impl_.SetStringField("contentDisposition", v);
  return *this;
}

ObjectMetadataPatchBuilder&
ObjectMetadataPatchBuilder::ResetContentDisposition() {
  impl_.RemoveField("contentDisposition");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetContentEncoding(
    std::string const& v) {
  if (v.empty()) {
    return ResetContentEncoding();
  }
  impl_.SetStringField("contentEncoding", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetContentEncoding() {
  impl_.RemoveField("contentEncoding");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetContentLanguage(
    std::string const& v) {
  if (v.empty()) {
    return ResetContentLanguage();
  }
  impl_.SetStringField("contentLanguage", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetContentLanguage() {
  impl_.RemoveField("contentLanguage");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetContentType(
    std::string const& v) {
  if (v.empty()) {
    return ResetContentType();
  }
  impl_.SetStringField("contentType", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetContentType() {
  impl_.RemoveField("contentType");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetEventBasedHold(
    bool v) {
  impl_.SetBoolField("eventBasedHold", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetEventBasedHold() {
  impl_.RemoveField("eventBasedHold");
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetMetadata(
    std::string const& key, std::string const& value) {
  metadata_subpatch_.SetStringField(key.c_str(), value);
  metadata_subpatch_dirty_ = true;
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetMetadata(
    std::string const& key) {
  metadata_subpatch_.RemoveField(key.c_str());
  metadata_subpatch_dirty_ = true;
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetMetadata() {
  metadata_subpatch_.clear();
  metadata_subpatch_dirty_ = true;
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::SetTemporaryHold(
    bool v) {
  impl_.SetBoolField("temporaryHold", v);
  return *this;
}

ObjectMetadataPatchBuilder& ObjectMetadataPatchBuilder::ResetTemporaryHold() {
  impl_.RemoveField("temporaryHold");
  return *this;
}

}  // namespace STORAGE_CLIENT_NS
}  // namespace storage
}  // namespace cloud
}  // namespace google
