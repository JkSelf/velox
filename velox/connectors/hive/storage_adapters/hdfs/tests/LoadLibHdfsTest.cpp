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
#include <cstdint>
#include <iostream>
#include "gtest/gtest.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsInternal.h"

using namespace facebook::velox;

class LoadLibHdfsTest : public testing::Test {
  void SetUp() override {}
};

// Before runing this test need export CLASSPATH=`$HADOOP_HOME/bin/hdfs
// classpath --glob`
TEST_F(LoadLibHdfsTest, loadLibHdfs) {
  filesystems::LibHdfsShim* driver_shim;
  arrow::Status msg = filesystems::ConnectLibHdfs(&driver_shim);
  if (!msg.ok()) {
    if (std::getenv("ARROW_HDFS_TEST_LIBHDFS_REQUIRE")) {
      FAIL() << "Loading libhdfs failed: " << msg.ToString();
    } else {
      std::cout << "Loading libhdfs failed, skipping tests gracefully: "
                << msg.ToString() << std::endl;
    }
    return;
  } else {
    std::cout << "Load libhdfs success" << std::endl;
  }

  // connect to HDFS with the builder object
  hdfsBuilder* builder = driver_shim->NewBuilder();
  driver_shim->BuilderSetNameNode(builder, "sr246");
  driver_shim->BuilderSetNameNodePort(builder, 9000);

  driver_shim->BuilderSetForceNewInstance(builder);
  auto fs = driver_shim->BuilderConnect(builder);

  auto testPath = "/tmp/hdfstest1";
  if (driver_shim->MakeDirectory(fs, testPath) == 0) {
    std::cout << "create hdfs path " << testPath << "\n";
  }

  if (driver_shim->Exists(fs, testPath) == 0) {
    std::cout << "the" << testPath << " path is existing"
              << "\n";
  } else {
    std::cout << "the" << testPath << " path is not existing"
              << "\n";
  }
}
