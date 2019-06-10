// Copyright 2019 Google LLC
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

#include "google/cloud/bigtable/completion_queue.h"
#include "google/cloud/bigtable/version.h"
#include "google/cloud/internal/invoke_result.h"

namespace google {
namespace cloud {
namespace bigtable {
inline namespace BIGTABLE_CLIENT_NS {
/**
 * A helper class for ensuring that a proper `CompletionQueue` is used.
 *
 * Its purpose is to wrap the optionally provided `CompletionQueue` and enforce
 * proper use of it, i.e.:
 * - if `CompletionQueue` is provided, pass it to any async operation's
 *   implementation and return an unmodified result
 * - if `CompletionQueue` is not provided, create a temporary one, wrap the
 *   result of whatever the implementation returns such that it shuts down the
 *   `CompletionQueue` on completion and execute `Run()` on the queue.
 */
class CompletionQueueProvider {
 public:
  CompletionQueueProvider(CompletionQueue cq) : cq_(std::move(cq)) {}
  CompletionQueueProvider() = default;

  template <typename Functor>
  google::cloud::internal::invoke_result_t<Functor, CompletionQueue&>
  WithCompletionQueue(Functor&& f) {
    if (cq_) {
      return f(*cq_);
    }
    CompletionQueue cq;
    auto ret = f(cq).then(
        [&cq](
            google::cloud::internal::invoke_result_t<Functor, CompletionQueue&>
                fut) {
          cq.Shutdown();
          return fut;
        });
    cq.Run();
    return ret;
  }

 private:
  optional<CompletionQueue> cq_;
};

}  // namespace BIGTABLE_CLIENT_NS
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
