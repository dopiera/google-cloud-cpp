// Copyright 2018 Google LLC.
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

#include "google/cloud/bigtable/internal/async_check_consistency.h"
#include "google/cloud/bigtable/admin_client.h"
#include "google/cloud/bigtable/internal/table.h"
#include "google/cloud/bigtable/testing/internal_table_test_fixture.h"
#include "google/cloud/bigtable/testing/mock_admin_client.h"
#include "google/cloud/bigtable/testing/mock_completion_queue.h"
#include "google/cloud/bigtable/testing/mock_response_reader.h"
#include "google/cloud/bigtable/testing/table_test_fixture.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include <gmock/gmock.h>
#include <thread>

namespace google {
namespace cloud {
namespace bigtable {
inline namespace BIGTABLE_CLIENT_NS {
namespace noex {
namespace {

namespace bt = ::google::cloud::bigtable;
namespace btproto = google::bigtable::admin::v2;
using namespace google::cloud::testing_util::chrono_literals;
using namespace ::testing;
using MockAsyncCheckConsistencyReader =
    google::cloud::bigtable::testing::MockAsyncResponseReader<
        btproto::CheckConsistencyResponse>;

class NoexAsyncCheckConsistencyTest : public ::testing::Test {};

/// @test Verify that TableAdmin::CheckConsistency() works in a simplest case.
TEST_F(NoexAsyncCheckConsistencyTest, Simple) {
  auto client = std::make_shared<testing::MockAdminClient>();
  auto impl = std::make_shared<testing::MockCompletionQueue>();
  bigtable::CompletionQueue cq(impl);

  auto reader =
      google::cloud::internal::make_unique<MockAsyncCheckConsistencyReader>();
  EXPECT_CALL(*reader, Finish(_, _, _))
      .WillOnce(Invoke([](btproto::CheckConsistencyResponse* response,
                          grpc::Status* status, void*) {
        response->set_consistent(true);
        *status = grpc::Status(grpc::StatusCode::OK, "mocked-status");
      }));

  EXPECT_CALL(*client, AsyncCheckConsistency(_, _, _))
      .WillOnce(
          Invoke([&reader](grpc::ClientContext*,
                           btproto::CheckConsistencyRequest const& request,
                           grpc::CompletionQueue*) {
            EXPECT_EQ("qwerty", request.consistency_token());
            // This is safe, see comments in MockAsyncResponseReader.
            return std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<
                btproto::CheckConsistencyResponse>>(reader.get());
          }));

  // Make the asynchronous request.
  bool op_called = false;
  grpc::Status capture_status;
  auto callback = [&op_called, &capture_status](CompletionQueue& cq,
                                                bool response,
                                                grpc::Status const& status) {
    EXPECT_TRUE(response);
    op_called = true;
    capture_status = status;
  };
  using OpType = internal::AsyncPollCheckConsistency<decltype(callback)>;
  auto polling_policy =
      bigtable::DefaultPollingPolicy(internal::kBigtableLimits);
  MetadataUpdatePolicy metadata_update_policy(
      "instance_id", MetadataParamTypes::NAME, "table_id");
  auto op = std::make_shared<OpType>(
      __func__, polling_policy->clone(), metadata_update_policy, client,
      ConsistencyToken("qwerty"), "table_name", std::move(callback));
  op->Start(cq);

  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());
  impl->SimulateCompletion(cq, true);

  EXPECT_TRUE(op_called);
  EXPECT_TRUE(impl->empty());

  EXPECT_TRUE(capture_status.ok());
  EXPECT_EQ("mocked-status", capture_status.error_message());
}

class NoexAsyncCheckConsistencyRetryTest
    : public bigtable::testing::internal::TableTestFixture,
      public WithParamInterface<bool> {};
// Verify that one retry works. No reason for many tests - AsyncPollOp is
// thoroughly tested.
TEST_P(NoexAsyncCheckConsistencyRetryTest, OneRetry) {
  bool const fail_first_rpc = GetParam();
  auto client = std::make_shared<testing::MockAdminClient>();
  auto impl = std::make_shared<testing::MockCompletionQueue>();
  bigtable::CompletionQueue cq(impl);

  auto reader =
      google::cloud::internal::make_unique<MockAsyncCheckConsistencyReader>();
  EXPECT_CALL(*reader, Finish(_, _, _))
      .WillOnce(Invoke([fail_first_rpc](
                           btproto::CheckConsistencyResponse* response,
                           grpc::Status* status, void*) {
        response->set_consistent(false);
        *status = grpc::Status(fail_first_rpc ? grpc::StatusCode::UNAVAILABLE
                                              : grpc::StatusCode::OK,
                               "mocked-status");
      }))
      .WillOnce(Invoke([](btproto::CheckConsistencyResponse* response,
                          grpc::Status* status, void*) {
        response->set_consistent(true);
        *status = grpc::Status(grpc::StatusCode::OK, "mocked-status");
      }));

  EXPECT_CALL(*client, AsyncCheckConsistency(_, _, _))
      .WillOnce(
          Invoke([&reader](grpc::ClientContext*,
                           btproto::CheckConsistencyRequest const& request,
                           grpc::CompletionQueue*) {
            EXPECT_EQ("qwerty", request.consistency_token());
            // This is safe, see comments in MockAsyncResponseReader.
            return std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<
                btproto::CheckConsistencyResponse>>(reader.get());
          }))
      .WillOnce(
          Invoke([&reader](grpc::ClientContext*,
                           btproto::CheckConsistencyRequest const& request,
                           grpc::CompletionQueue*) {
            EXPECT_EQ("qwerty", request.consistency_token());
            // This is safe, see comments in MockAsyncResponseReader.
            return std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<
                btproto::CheckConsistencyResponse>>(reader.get());
          }));

  // Make the asynchronous request.
  bool op_called = false;
  grpc::Status capture_status;
  auto callback = [&op_called, &capture_status](CompletionQueue& cq,
                                                bool response,
                                                grpc::Status const& status) {
    EXPECT_TRUE(response);
    op_called = true;
    capture_status = status;
  };
  using OpType = internal::AsyncPollCheckConsistency<decltype(callback)>;
  auto polling_policy =
      bigtable::DefaultPollingPolicy(internal::kBigtableLimits);
  MetadataUpdatePolicy metadata_update_policy(
      "instance_id", MetadataParamTypes::NAME, "table_id");
  auto op = std::make_shared<OpType>(
      __func__, polling_policy->clone(), metadata_update_policy, client,
      ConsistencyToken("qwerty"), "table_name", std::move(callback));
  op->Start(cq);

  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());  // request
  impl->SimulateCompletion(cq, true);
  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());  // timer

  impl->SimulateCompletion(cq, true);
  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());  //  request

  impl->SimulateCompletion(cq, true);

  EXPECT_TRUE(op_called);
  EXPECT_TRUE(impl->empty());

  EXPECT_TRUE(capture_status.ok());
  EXPECT_EQ("mocked-status", capture_status.error_message());
}

INSTANTIATE_TEST_CASE_P(OneRetry, NoexAsyncCheckConsistencyRetryTest,
                        ::testing::Values(false, true));

}  // namespace
}  // namespace noex
}  // namespace BIGTABLE_CLIENT_NS
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
