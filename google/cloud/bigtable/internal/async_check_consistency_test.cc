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
#include "google/cloud/bigtable/internal/table_admin.h"
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
using MockAsyncGenerateConsistencyToken =
    google::cloud::bigtable::testing::MockAsyncResponseReader<
        btproto::GenerateConsistencyTokenResponse>;

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

class EndToEndConfig {
 public:
  grpc::StatusCode error_code1;
  bool second_call;
  grpc::StatusCode error_code2;
  bool finished;
  grpc::StatusCode expected;
};

class NoexAsyncCheckConsistencyEndToEnd
    : public bigtable::testing::internal::TableTestFixture,
      public WithParamInterface<EndToEndConfig> {};

TEST_P(NoexAsyncCheckConsistencyEndToEnd, EndToEnd) {
  using namespace ::testing;

  auto config = GetParam();

  std::string const kProjectId = "the-project";
  std::string const kInstanceId = "the-instance";
  std::string const kClusterId = "the-cluster";
  bigtable::TableId const kTableId("the-table");

  internal::RPCPolicyParameters const noRetries = {
      std::chrono::hours(0),
      std::chrono::hours(0),
      std::chrono::hours(0),
  };
  auto polling_policy = bigtable::DefaultPollingPolicy(noRetries);

  auto client = std::make_shared<testing::MockAdminClient>();
  EXPECT_CALL(*client, project()).WillRepeatedly(ReturnRef(kProjectId));
  bigtable::noex::TableAdmin tested(client, kInstanceId, *polling_policy);
  auto impl = std::make_shared<testing::MockCompletionQueue>();
  bigtable::CompletionQueue cq(impl);

  auto token_reader =
      google::cloud::internal::make_unique<MockAsyncGenerateConsistencyToken>();
  EXPECT_CALL(*token_reader, Finish(_, _, _))
      .WillOnce(
          Invoke([config](btproto::GenerateConsistencyTokenResponse* response,
                          grpc::Status* status, void*) {
            response->set_consistency_token("qwerty");
            *status = grpc::Status(config.error_code1, "mocked-status");
          }));
  auto reader =
      google::cloud::internal::make_unique<MockAsyncCheckConsistencyReader>();
  if (config.second_call) {
    EXPECT_CALL(*reader, Finish(_, _, _))
        .WillOnce(Invoke([config](btproto::CheckConsistencyResponse* response,
                                  grpc::Status* status, void*) {
          response->set_consistent(config.finished);
          *status = grpc::Status(config.error_code2, "mocked-status");
        }));
  }

  EXPECT_CALL(*client, AsyncGenerateConsistencyToken(_, _, _))
      .WillOnce(
          Invoke([&token_reader](
                     grpc::ClientContext*,
                     btproto::GenerateConsistencyTokenRequest const& request,
                     grpc::CompletionQueue*) {
            // This is safe, see comments in MockAsyncResponseReader.
            return std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<
                btproto::GenerateConsistencyTokenResponse>>(token_reader.get());
          }));
  if (config.second_call) {
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
  }

  // Make the asynchronous request.
  bool op_called = false;
  grpc::Status capture_status;
  auto callback = [&op_called, &capture_status](CompletionQueue& cq,
                                                grpc::Status const& status) {
    op_called = true;
    capture_status = status;
  };
  tested.AsyncAwaitConsistency(kTableId, cq, callback);

  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());  // AsyncGenerateConsistencyToken
  impl->SimulateCompletion(cq, true);

  if (config.second_call) {
    EXPECT_FALSE(op_called);
    EXPECT_EQ(1U, impl->size());  // AsyncCheckConsistency
    impl->SimulateCompletion(cq, true);
  }

  EXPECT_TRUE(op_called);
  EXPECT_TRUE(impl->empty());

  EXPECT_EQ(config.expected, capture_status.error_code());
}

INSTANTIATE_TEST_CASE_P(
    EndToEnd, NoexAsyncCheckConsistencyEndToEnd,
    ::testing::Values(
        // Everything succeeds immediately.
        EndToEndConfig{grpc::StatusCode::OK, true, grpc::StatusCode::OK, true,
                       grpc::StatusCode::OK},
        // Generating token fails, the error should be propagated.
        EndToEndConfig{grpc::StatusCode::PERMISSION_DENIED, false,
                       grpc::StatusCode::UNKNOWN, true,
                       grpc::StatusCode::PERMISSION_DENIED},
        // CheckConsistency times out w.r.t PollingPolicy, UNKNOWN is returned.
        EndToEndConfig{grpc::StatusCode::OK, true, grpc::StatusCode::OK, false,
                       grpc::StatusCode::UNKNOWN},
        // CheckConsistency fails. UNKNOWN is returned.
        EndToEndConfig{grpc::StatusCode::OK, true,
                       grpc::StatusCode::UNAVAILABLE, false,
                       grpc::StatusCode::UNAVAILABLE},
        // CheckConsistency succeeds but reports an error UNKNOWN is returned -
        // that's a bug simulation
        EndToEndConfig{grpc::StatusCode::OK, true,
                       grpc::StatusCode::UNAVAILABLE, true,
                       grpc::StatusCode::UNAVAILABLE}));

class CancelConfig {
 public:
  // Error code returned from GenerateConsistencyToken
  grpc::StatusCode error_code1;
  // Whether to call cancel during the first operation.
  bool cancel1;
  // Whether to expect a call to CheckConsistency
  bool second_call;
  // Error code returned from CheckConsistency
  grpc::StatusCode error_code2;
  // Whether to call cancel during the first operation.
  bool cancel2;
  grpc::StatusCode expected;
};

class NoexAsyncCheckConsistencyCancel
    : public bigtable::testing::internal::TableTestFixture,
      public WithParamInterface<CancelConfig> {};

TEST_P(NoexAsyncCheckConsistencyCancel, Simple) {
  using namespace ::testing;

  auto config = GetParam();

  std::string const kProjectId = "the-project";
  std::string const kInstanceId = "the-instance";
  std::string const kClusterId = "the-cluster";
  bigtable::TableId const kTableId("the-table");

  internal::RPCPolicyParameters const noRetries = {
      std::chrono::hours(0),
      std::chrono::hours(0),
      std::chrono::hours(0),
  };
  auto polling_policy = bigtable::DefaultPollingPolicy(noRetries);

  auto client = std::make_shared<testing::MockAdminClient>();
  EXPECT_CALL(*client, project()).WillRepeatedly(ReturnRef(kProjectId));
  bigtable::noex::TableAdmin tested(client, kInstanceId, *polling_policy);
  auto impl = std::make_shared<testing::MockCompletionQueue>();
  bigtable::CompletionQueue cq(impl);

  auto token_reader =
      google::cloud::internal::make_unique<MockAsyncGenerateConsistencyToken>();
  EXPECT_CALL(*token_reader, Finish(_, _, _))
      .WillOnce(
          Invoke([config](btproto::GenerateConsistencyTokenResponse* response,
                          grpc::Status* status, void*) {
            response->set_consistency_token("qwerty");
            *status = grpc::Status(config.error_code1, "mocked-status");
          }));
  auto reader =
      google::cloud::internal::make_unique<MockAsyncCheckConsistencyReader>();
  if (config.second_call) {
    EXPECT_CALL(*reader, Finish(_, _, _))
        .WillOnce(Invoke([config](btproto::CheckConsistencyResponse* response,
                                  grpc::Status* status, void*) {
          response->set_consistent(true);
          *status = grpc::Status(config.error_code2, "mocked-status");
        }));
  }

  EXPECT_CALL(*client, AsyncGenerateConsistencyToken(_, _, _))
      .WillOnce(
          Invoke([&token_reader](
                     grpc::ClientContext*,
                     btproto::GenerateConsistencyTokenRequest const& request,
                     grpc::CompletionQueue*) {
            // This is safe, see comments in MockAsyncResponseReader.
            return std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<
                btproto::GenerateConsistencyTokenResponse>>(token_reader.get());
          }));
  if (config.second_call) {
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
  }

  // Make the asynchronous request.
  bool op_called = false;
  grpc::Status capture_status;
  auto callback = [&op_called, &capture_status](CompletionQueue& cq,
                                                grpc::Status const& status) {
    op_called = true;
    capture_status = status;
  };
  auto op = tested.AsyncAwaitConsistency(kTableId, cq, callback);

  EXPECT_FALSE(op_called);
  EXPECT_EQ(1U, impl->size());  // AsyncGenerateConsistencyToken
  if (config.cancel1) {
    op->Cancel();
  }

  impl->SimulateCompletion(cq, true);

  if (config.second_call) {
    EXPECT_FALSE(op_called);
    EXPECT_EQ(1U, impl->size());  // AsyncCheckConsistency
    if (config.cancel1) {
      op->Cancel();
    }
    impl->SimulateCompletion(cq, true);
  }

  EXPECT_TRUE(op_called);
  EXPECT_TRUE(impl->empty());

  EXPECT_EQ(config.expected, capture_status.error_code());
}

INSTANTIATE_TEST_CASE_P(CancelTest, NoexAsyncCheckConsistencyCancel,
                        ::testing::Values(
                            // Cancel during GenerateConsistencyTokenResponse
                            // yields the request returning CANCELLED.
                            CancelConfig{grpc::StatusCode::CANCELLED, true,
                                         false, grpc::StatusCode::UNKNOWN,
                                         false, grpc::StatusCode::CANCELLED},
                            // Cancel during GenerateConsistencyTokenResponse
                            // yields the request returning OK.
                            CancelConfig{grpc::StatusCode::OK, true, false,
                                         grpc::StatusCode::UNKNOWN, false,
                                         grpc::StatusCode::CANCELLED},
                            // Cancel during CheckConsistency
                            // yields the request returning CANCELLED.
                            CancelConfig{grpc::StatusCode::OK, false, true,
                                         grpc::StatusCode::CANCELLED, true,
                                         grpc::StatusCode::CANCELLED},
                            // Cancel during CheckConsistency
                            // yields the request returning OK.
                            CancelConfig{grpc::StatusCode::OK, false, true,
                                         grpc::StatusCode::OK, true,
                                         grpc::StatusCode::OK}));

//   grpc::StatusCode error_code1;
//   bool cancel1;
//   bool second_call;
//   grpc::StatusCode error_code2;
//   bool cancel2;
//   grpc::StatusCode expected;

}  // namespace
}  // namespace noex
}  // namespace BIGTABLE_CLIENT_NS
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
