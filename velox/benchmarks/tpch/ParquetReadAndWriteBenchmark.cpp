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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

#include <filesystem>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}

static bool validateDataFormat(const char* flagname, const std::string& value) {
  if ((value.compare("parquet") == 0) || (value.compare("orc") == 0)) {
    return true;
  }
  std::cout
      << fmt::format(
             "Invalid value for --{}: {}. Allowed values are [\"parquet\", \"orc\"]",
             flagname,
             value)
      << std::endl;
  return false;
}

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}
} // namespace

DEFINE_string(data_path, "", "Root path of TPC-H data");
DEFINE_int32(
    run_query_verbose,
    -1,
    "Run a given query and print execution statistics");
DEFINE_bool(
    include_custom_stats,
    false,
    "Include custom statistics along with execution statistics");
DEFINE_int32(num_drivers, 4, "Number of drivers");
DEFINE_string(data_format, "parquet", "Data format");
DEFINE_int32(num_splits_per_file, 10, "Number of splits per file");

/// DEFINE_validator(data_path, &notEmpty);
// DEFINE_validator(data_format, &validateDataFormat);

void ReadAndWrite(std::string read_file_name, std::string write_file_name) {
  dwio::common::ReaderOptions readerOptions;
  facebook::velox::parquet::ParquetReader reader(
      std::make_unique<FileInputStream>(read_file_name), readerOptions);

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
      write_file_name);
  auto config = std::make_shared<facebook::velox::dwrf::Config>();
  // set the snappy compression velox only support zlib and zstd
  // config->set(Config::COMPRESSION, CompressionKind::CompressionKind_ZSTD);
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

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  parquet::registerParquetReaderFactory();
  std::cout << "the read path is " << argv[1] << " and the write path is "
            << argv[2] << "\n";

  std::string read_path = argv[1];
  std::string write_path = argv[2];

  std::string command = "touch " + write_path;
  system(command.c_str());

  ReadAndWrite(read_path, write_path);

  // for (const auto entry : std::filesystem::directory_iterator(read_path)) {
  //   // file_name end with .parquet
  //   std::string read_file_path = entry.path();
  //   std::string ending = "";
  //   if (read_file_path.length() >= 7) {
  //     ending = read_file_path.substr(read_file_path.length() - 7, 7);
  //   }

  //   if (ending.compare("parquet") == 0) {
  //     // construct the write file path
  //     auto last_pos = read_file_path.find_last_of("/");
  //     std::string file_name = read_file_path.substr(
  //         last_pos + 1, (read_file_path.length() - last_pos));
  //     std::string write_file_path = write_path + "/" + file_name;
  //     std::string command = "touch " + write_file_path;
  //     std::cout << "the read_file_path is " << read_file_path
  //               << " and the write file path is " << write_file_path << "\n";
  //     system(command.c_str());

  //     ReadAndWrite(read_file_path, write_file_path);
  //   }
  // }
}
