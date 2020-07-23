// Copyright 2020 Google LLC
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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_PUBSUB_INTERNAL_BATCHING_PUBLISHER_CONNECTION_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_PUBSUB_INTERNAL_BATCHING_PUBLISHER_CONNECTION_H

#include "google/cloud/pubsub/publisher_connection.h"
#include "google/cloud/pubsub/version.h"
#include <mutex>

namespace google {
namespace cloud {
namespace pubsub_internal {
inline namespace GOOGLE_CLOUD_CPP_PUBSUB_NS {

class BatchingPublisherConnection
    : public pubsub::PublisherConnection,
      public std::enable_shared_from_this<BatchingPublisherConnection> {
 public:
  static std::shared_ptr<BatchingPublisherConnection> Create(
      pubsub::Topic topic, pubsub::BatchingConfig batching_config,
      std::shared_ptr<pubsub_internal::PublisherStub> stub,
      pubsub::ConnectionOptions const& connection_options) {
    return std::shared_ptr<BatchingPublisherConnection>(
        new BatchingPublisherConnection(std::move(topic),
                                        std::move(batching_config),
                                        std::move(stub), connection_options));
  }

  future<StatusOr<std::string>> Publish(PublishParams p) override;

 private:
  explicit BatchingPublisherConnection(
      pubsub::Topic topic, pubsub::BatchingConfig batching_config,
      std::shared_ptr<pubsub_internal::PublisherStub> stub,
      pubsub::ConnectionOptions const& connection_options)
      : topic_(std::move(topic)),
        topic_full_name_(topic_.FullName()),
        batching_config_(std::move(batching_config)),
        stub_(std::move(stub)),
        background_(connection_options.background_threads_factory()()),
        cq_(background_->cq()) {}

  void OnTimer();
  void MaybeFlush(std::unique_lock<std::mutex> lk);
  void Flush(std::unique_lock<std::mutex> lk);

  pubsub::Topic topic_;
  std::string topic_full_name_;
  pubsub::BatchingConfig batching_config_;
  std::shared_ptr<pubsub_internal::PublisherStub> stub_;
  std::unique_ptr<BackgroundThreads> background_;
  google::cloud::CompletionQueue cq_;

  std::mutex mu_;
  struct Item {
    promise<StatusOr<std::string>> response;
    pubsub::Message message;
  };
  std::vector<Item> pending_;
  std::chrono::system_clock::time_point batch_expiration_;
};

}  // namespace GOOGLE_CLOUD_CPP_PUBSUB_NS
}  // namespace pubsub_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_PUBSUB_INTERNAL_BATCHING_PUBLISHER_CONNECTION_H