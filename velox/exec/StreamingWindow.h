/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/Window.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

/// This operator differs from the Window operator in that it does not require
/// sorting the input vector or caching all the input data. Instead, it groups
/// the input batch, performs calculations on first n-1 groups, and carries
/// forward the last group to the next input batch.
class StreamingWindow : public Window {
 public:
  StreamingWindow(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::WindowNode>& windowNode)
      : Window(operatorId, driverCtx, windowNode) {}

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

  void noMoreInput() override;

  void createPeerAndFrameBuffers() override;

 private:
  RowVectorPtr createOutput();

  // Get the output vector based on the num rows per batch.
  RowVectorPtr getResult();

  void updateValues();

  RowVectorPtr output_ = nullptr;

  vector_size_t numPartitions_ = 0;

  std::vector<char*> wholeGroupdRows_;
  vector_size_t wholeGroupdNumRows_ = 0;

  vector_size_t outputBatchSize_ = 0;

  char* preRow_ = nullptr;
};

} // namespace facebook::velox::exec
