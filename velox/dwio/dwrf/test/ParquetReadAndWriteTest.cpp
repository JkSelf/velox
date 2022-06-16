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

#include <folly/Random.h>
#include <random>
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/dwio/dwrf/test/utils/MapBuilder.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using facebook::velox::memory::MemoryPool;
using folly::Random;

constexpr uint64_t kSizeMB = 1024UL * 1024UL;

TEST(ParquetReadAndWriteTest, TestParquetReaderAndWrite) {
  parquet::registerParquetReaderFactory();

  dwio::common::ReaderOptions readerOptions;
  facebook::velox::parquet::ParquetReader reader(
      std::make_unique<FileInputStream>(
          "/mnt/DP_Disk8/jk/parquet_data/lineitem/part-00054-b8cf32bd-aa8e-4ae5-b37b-7b41bf1d9328-c000.snappy.parquet"),
      readerOptions);

  auto rowType = reader.rowType();
  auto scanSpec = std::make_shared<common::ScanSpec>("");
  for (auto i = 0; i < rowType->size(); ++i) {
    auto child =
        scanSpec->getOrCreateChild(common::Subfield(rowType->nameOf(i)));
    child->setProjectOut(true);
    child->setChannel(i);
  }

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(
      std::make_shared<ColumnSelector>(rowType, rowType->names()));
  rowReaderOpts.setScanSpec(scanSpec);

  auto rowReader = reader.createRowReader(rowReaderOpts);

  auto sink = std::make_unique<facebook::velox::dwio::common::FileSink>(
      "/mnt/DP_Disk8/jk/dwrf_data/part-00054-b8cf32bd-aa8e-4ae5-b37b-7b41bf1d9328-c000.snappy.parquet");
  auto config = std::make_shared<facebook::velox::dwrf::Config>();
  // set the snappy compression velox only support zlib and zstd
  config->set(Config::COMPRESSION, CompressionKind::CompressionKind_ZSTD);
  const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max();
  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = rowType;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicyFactory = nullptr;
  options.layoutPlannerFactory = nullptr;
  auto writer = std::make_unique<facebook::velox::dwrf::Writer>(
      options,
      std::move(sink),
      facebook::velox::memory::getProcessDefaultMemoryManager().getRoot());

  VectorPtr batch;
  auto num_rows = reader.numberOfRows();
  while (rowReader->next(num_rows.value(), batch)) {
    writer->write(batch);
  }
  writer->close();
}
