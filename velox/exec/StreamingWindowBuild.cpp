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

#include "velox/exec/StreamingWindowBuild.h"
#include <iostream>

namespace facebook::velox::exec {

StreamingWindowBuild::StreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool)
    : WindowBuild(windowNode, pool) {
  allKeyInfo_.reserve(partitionKeyInfo_.size() + sortKeyInfo_.size());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), partitionKeyInfo_.begin(), partitionKeyInfo_.end());
  allKeyInfo_.insert(
      allKeyInfo_.cend(), sortKeyInfo_.begin(), sortKeyInfo_.end());
  partitionStartRows_.resize(0);
}

void StreamingWindowBuild::updatePartitions() {
  partitionStartRows_.push_back(sortedRows_.size());
  sortedRows_.insert(
      sortedRows_.end(), partitionRows_.begin(), partitionRows_.end());
  partitionRows_.clear();
}

void StreamingWindowBuild::addInput(RowVectorPtr input) {
  std::cout << "StreamingWindowBuild::addInput is running"
            << "\n";
  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedInputVectors_[col].decode(*input->childAt(col));
  }

  auto partitionCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, partitionKeyInfo_);
  };

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (previousRow_ != nullptr && partitionCompare(previousRow_, newRow)) {
      updatePartitions();
    }

    partitionRows_.push_back(newRow);
    previousRow_ = newRow;
  }

  if (sortedRows_.size() > 0) {
    partitionStartRows_.push_back(sortedRows_.size());
  }

  numRows_ += input->size();
}

void StreamingWindowBuild::noMoreInput() {
  if (numRows_ == 0) {
    return;
  }

  noMoreInput_ = true;
}

std::unique_ptr<WindowPartition> StreamingWindowBuild::nextPartition() {
  if (noMoreInput_) {
    // Process the last partition.
    updatePartitions();
    partitionStartRows_.push_back(sortedRows_.size());
  }

  currentPartition_++;
  if (sortedRows_.size() == 0 ||
      currentPartition_ > partitionStartRows_.size() - 2) {
    currentPartition_ = -1;
    // All partitions are output. No more partitions available.
    return nullptr;
  }

  auto windowPartition = std::make_unique<WindowPartition>(
      data_.get(), inputColumns_, sortKeyInfo_);
  // There is partition data available now.
  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);
  windowPartition->resetPartition(partition);

  return windowPartition;
}

bool StreamingWindowBuild::hasNextPartition() {
  return data_->numRows() != 0;
}

} // namespace facebook::velox::exec
