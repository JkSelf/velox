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
#include "velox/exec/StreamingWindow.h"
#include <iostream>
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

void StreamingWindow::updateValues() {
  partitionStartRows_.push_back(sortedRows_.size());
  sortedRows_.insert(
      sortedRows_.end(), wholeGroupdRows_.begin(), wholeGroupdRows_.end());
  numPartitions_ += 1;
  wholeGroupdRows_.clear();
}

void StreamingWindow::addInput(RowVectorPtr input) {
  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedInputVectors_[col].decode(*input->childAt(col));
  }

  auto partitionCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, partitionKeyInfo_);
  };

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (preRow_ != nullptr && partitionCompare(preRow_, newRow)) {
      // the wholeGroupdRows_ already store whole group, we need to put the
      // wholeGroupdRows_ into the sortedRows and the partitionStartRows_

      updateValues();
    }

    wholeGroupdRows_.push_back(newRow);
    preRow_ = newRow;
  }

  partitionStartRows_.push_back(sortedRows_.size());

  outputBatchSize_ = outputBatchRows(data_->estimateRowSize());
}

void StreamingWindow::createPeerAndFrameBuffers() {
  // TODO: This computation needs to be revised. It only takes into account
  // the input columns size. We need to also account for the output columns.
  // numRowsPerOutput_ = outputBatchRows(data_->estimateRowSize());

  numRowsPerOutput_ = numRows_;

  peerStartBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());
  peerEndBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());

  auto numFuncs = windowFunctions_.size();

  frameStartBuffers_.clear();
  frameEndBuffers_.clear();
  validFrames_.clear();
  frameStartBuffers_.reserve(numFuncs);
  frameEndBuffers_.reserve(numFuncs);
  validFrames_.reserve(numFuncs);

  for (auto i = 0; i < numFuncs; i++) {
    BufferPtr frameStartBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    BufferPtr frameEndBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    frameStartBuffers_.push_back(frameStartBuffer);
    frameEndBuffers_.push_back(frameEndBuffer);
    validFrames_.push_back(SelectivityVector(numRowsPerOutput_));
  }
}

void StreamingWindow::noMoreInput() {
  Operator::noMoreInput();
}

bool StreamingWindow::isFinished() {
  return noMoreInput_ && data_->numRows() == 0 && output_ == nullptr;
}

RowVectorPtr StreamingWindow::getResult() {
  if (output_->size() > outputBatchSize_) {
    auto result = std::dynamic_pointer_cast<RowVector>(
        output_->slice(0, outputBatchSize_));
    auto remainingSize = output_->size() - outputBatchSize_;
    output_ = std::dynamic_pointer_cast<RowVector>(
        output_->slice(outputBatchSize_, remainingSize));
    return result;
  } else if (noMoreInput_) {
    auto result = std::move(output_);
    return result;
  } else {
    return nullptr;
  }
}

RowVectorPtr StreamingWindow::createOutput() {
  numRows_ = sortedRows_.size();

  currentPartition_ = 0;
  numProcessedRows_ = 0;

  createPeerAndFrameBuffers();

  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numRows_, operatorCtx_->pool()));

  // Set all passthrough input columns.
  for (int i = 0; i < numInputColumns_; ++i) {
    data_->extractColumn(sortedRows_.data(), numRows_, i, result->childAt(i));
  }

  // Construct vectors for the window function output columns.
  std::vector<VectorPtr> windowOutputs;
  windowOutputs.reserve(windowFunctions_.size());
  for (int i = numInputColumns_; i < outputType_->size(); i++) {
    auto output = BaseVector::create(
        outputType_->childAt(i), numRows_, operatorCtx_->pool());
    windowOutputs.emplace_back(std::move(output));
  }

  // Compute the output values of window functions.
  callApplyLoop(numRows_, windowOutputs);

  for (int j = numInputColumns_; j < outputType_->size(); j++) {
    result->childAt(j) = windowOutputs[j - numInputColumns_];
  }

  // clear the sortedRows_ and the partitionStartRows_;
  data_->eraseRows(
      folly::Range<char**>(sortedRows_.data(), sortedRows_.size()));
  sortedRows_.clear();
  partitionStartRows_.clear();

  if (output_ == nullptr) {
    output_ = result;
  } else {
    output_->append(result.get());
  }
  return getResult();
}

RowVectorPtr StreamingWindow::getOutput() {
  if (noMoreInput_) {
    // Handle the last group in last input batch
    updateValues();
    partitionStartRows_.push_back(sortedRows_.size());
  }

  if (sortedRows_.size() > 0) {
    return createOutput();
  } else if (noMoreInput_ && output_ != nullptr) {
    return getResult();
  } else {
    return nullptr;
  }
}

} // namespace facebook::velox::exec
