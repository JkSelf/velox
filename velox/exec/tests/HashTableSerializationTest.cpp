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

#include "velox/exec/HashTable.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <sys/wait.h>
#include <unistd.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace facebook::velox::exec::test {

class HashTableSerializationTest : public testing::Test,
                                    public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    pool_.reset();
  }

  // 创建测试用的 HashTable
  std::unique_ptr<HashTable<true>> createTestHashTable(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates = false) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    for (int i = 0; i < keyTypes.size(); ++i) {
      hashers.push_back(std::make_unique<VectorHasher>(keyTypes[i], i));
    }

    return std::make_unique<HashTable<true>>(
        std::move(hashers),
        std::vector<Accumulator>{},
        dependentTypes,
        allowDuplicates,
        true,  // isJoinBuild
        false, // hasProbedFlag
        false, // hasCountFlag
        0,     // minTableSizeForParallelJoinBuild
        pool_.get());
  }

  // 向 HashTable 插入数据
  void insertData(
      HashTable<true>* table,
      const RowVectorPtr& data,
      const std::vector<column_index_t>& /*keyChannels*/) {
    SelectivityVector allRows(data->size());

    // 分配行并插入
    std::vector<char*> inserted(data->size());
    const auto nextOffset = table->rows()->nextOffset();
    for (int i = 0; i < data->size(); ++i) {
      inserted[i] = table->rows()->newRow();
      if (nextOffset > 0) {
        *reinterpret_cast<char**>(inserted[i] + nextOffset) = nullptr;
      }
    }
    
    // 复制数据到行
    for (int col = 0; col < data->childrenSize(); ++col) {
      DecodedVector decoded(*data->childAt(col), allRows);
      for (int row = 0; row < data->size(); ++row) {
        table->rows()->store(decoded, row, inserted[row], col);
      }
    }
  }

  // 验证两个 HashTable 的内容是否相同
  void verifyHashTablesEqual(
      HashTable<true>* original,
      HashTable<true>* restored) {
    // 验证基本元数据
    EXPECT_EQ(original->numDistinct(), restored->numDistinct());
    EXPECT_EQ(original->hashMode(), restored->hashMode());
    
    // 验证行数据
    auto* origRows = original->rows();
    auto* restRows = restored->rows();
    
    ASSERT_EQ(origRows->numRows(), restRows->numRows());
    ASSERT_EQ(origRows->columnTypes().size(), restRows->columnTypes().size());
    
    // 获取所有行并比较
    std::vector<char*> origRowPtrs;
    std::vector<char*> restRowPtrs;
    
    RowContainerIterator origIter;
    RowContainerIterator restIter;
    
    std::vector<char*> buffer(1000);
    
    while (true) {
      auto numRows = origRows->listRows(
          &origIter, buffer.size(), RowContainer::kUnlimited, buffer.data());
      if (numRows == 0) break;
      origRowPtrs.insert(origRowPtrs.end(), buffer.begin(), buffer.begin() + numRows);
    }
    
    while (true) {
      auto numRows = restRows->listRows(
          &restIter, buffer.size(), RowContainer::kUnlimited, buffer.data());
      if (numRows == 0) break;
      restRowPtrs.insert(restRowPtrs.end(), buffer.begin(), buffer.begin() + numRows);
    }
    
    ASSERT_EQ(origRowPtrs.size(), restRowPtrs.size());

    auto encodeRows = [](RowContainer* rows, const std::vector<char*>& rowPtrs) {
      std::vector<std::string> encodedRows;
      encodedRows.reserve(rowPtrs.size());
      for (auto* rowPtr : rowPtrs) {
        std::string encoded;
        for (int col = 0; col < rows->columnTypes().size(); ++col) {
          const auto column = rows->columnAt(col);
          const bool isNull = RowContainer::isNullAt(rowPtr, column);
          encoded += isNull ? "NULL:" : "VAL:";
          if (!isNull) {
            const auto& type = rows->columnTypes()[col];
            switch (type->kind()) {
              case TypeKind::BIGINT:
                encoded += std::to_string(
                    *reinterpret_cast<const int64_t*>(rowPtr + column.offset()));
                break;
              case TypeKind::INTEGER:
                encoded += std::to_string(
                    *reinterpret_cast<const int32_t*>(rowPtr + column.offset()));
                break;
              case TypeKind::DOUBLE:
                encoded += std::to_string(
                    *reinterpret_cast<const double*>(rowPtr + column.offset()));
                break;
              case TypeKind::BOOLEAN:
                encoded +=
                    *reinterpret_cast<const bool*>(rowPtr + column.offset())
                    ? "true"
                    : "false";
                break;
              case TypeKind::VARCHAR: {
                const auto* str = reinterpret_cast<const StringView*>(
                    rowPtr + column.offset());
                encoded.append(str->data(), str->size());
                break;
              }
              default:
                VELOX_FAIL(
                    "Unsupported type in HashTableSerializationTest comparison: {}",
                    type->toString());
            }
          }
          encoded += "|";
        }
        encodedRows.push_back(std::move(encoded));
      }
      std::sort(encodedRows.begin(), encodedRows.end());
      return encodedRows;
    };

    EXPECT_EQ(encodeRows(origRows, origRowPtrs), encodeRows(restRows, restRowPtrs));
  }

  void verifyJoinProbe(
      HashTable<true>* table,
      const RowVectorPtr& probe,
      int32_t expectedHits) {
    HashLookup lookup(table->hashers(), pool_.get());
    SelectivityVector rows(probe->size());
    rows.setAll();

    table->prepareForJoinProbe(lookup, probe, rows, true);
    table->joinProbe(lookup);

    int32_t hitCount = 0;
    for (int32_t row = 0; row < probe->size(); ++row) {
      if (lookup.hits[row] != nullptr) {
        ++hitCount;
      }
    }
    EXPECT_EQ(hitCount, expectedHits);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<TempDirectoryPath> tempDir_;
};

// ============================================================================
// 基础序列化测试
// ============================================================================

TEST_F(HashTableSerializationTest, BasicSerializationDefault) {
  // 创建简单的 HashTable
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  // 插入测试数据
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e"})
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  // 序列化
  std::stringstream ss;
  table->serialize(ss);
  
  // 反序列化
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  // 验证
  verifyHashTablesEqual(table.get(), restored.get());
}

TEST_F(HashTableSerializationTest, BasicSerializationJoinProbe) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e"})
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
  verifyJoinProbe(restored.get(), data, data->size());
}

// ============================================================================
// 多种数据类型测试
// ============================================================================

TEST_F(HashTableSerializationTest, MultipleDataTypes) {
  auto table = createTestHashTable(
      {BIGINT(), INTEGER(), VARCHAR()},
      {DOUBLE(), BOOLEAN()},
      false);
  
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<int32_t>({10, 20, 30}),
      makeFlatVector<std::string>({"key1", "key2", "key3"}),
      makeFlatVector<double>({1.1, 2.2, 3.3}),
      makeFlatVector<bool>({true, false, true})
  });
  
  insertData(table.get(), data, {0, 1, 2});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
}

// ============================================================================
// NULL 值测试
// ============================================================================

TEST_F(HashTableSerializationTest, NullValues) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt, 5}),
      makeNullableFlatVector<std::string>(
          {"a", std::nullopt, "c", "d", std::nullopt})
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
}

// ============================================================================
// 大数据量测试
// ============================================================================

TEST_F(HashTableSerializationTest, LargeDataSet) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  // 创建 10000 行数据
  std::vector<int64_t> keys;
  std::vector<std::string> values;
  for (int i = 0; i < 10000; ++i) {
    keys.push_back(i);
    values.push_back("value_" + std::to_string(i));
  }
  
  auto data = makeRowVector({
      makeFlatVector(keys),
      makeFlatVector(values)
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
}

// ============================================================================
// 长字符串测试（测试内联 vs 外部分配）
// ============================================================================

TEST_F(HashTableSerializationTest, LongStrings) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  // 创建包含短字符串和长字符串的数据
  std::vector<std::string> values = {
      "short",  // 内联
      "this is a very long string that exceeds 12 bytes",  // 外部分配
      "medium",
      std::string(1000, 'x'),  // 非常长的字符串
      ""  // 空字符串
  };
  
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector(values)
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
}

// ============================================================================
// 允许重复键测试
// ============================================================================

TEST_F(HashTableSerializationTest, AllowDuplicates) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, true);
  
  // 插入重复的键
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2, 3}),
      makeFlatVector<std::string>({"a1", "a2", "b1", "b2", "c"})
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  auto restored = HashTable<true>::deserialize(ss, pool_.get());
  
  verifyHashTablesEqual(table.get(), restored.get());
}

// ============================================================================
// 跨进程序列化测试（模拟集群环境）
// ============================================================================

TEST_F(HashTableSerializationTest, CrossProcessSerialization) {
  // 创建临时文件
  std::string tempFile = tempDir_->getPath() + "/hashtable_cross_process.bin";
  
  // 父进程：创建并序列化 HashTable
  pid_t pid = fork();
  
  if (pid == 0) {
    // 子进程：序列化
    auto childPool = memory::memoryManager()->addLeafPool();
    auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
    
    auto data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeFlatVector<std::string>({"child1", "child2", "child3", "child4", "child5"})
    });
    
    insertData(table.get(), data, {0});
    table->prepareJoinTable(
        {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
    
    // 序列化到文件
    std::ofstream out(tempFile, std::ios::binary);
    table->serialize(out);
    out.close();
    
    exit(0);
  } else {
    // 父进程：等待子进程完成
    int status;
    waitpid(pid, &status, 0);
    ASSERT_EQ(WEXITSTATUS(status), 0) << "Child process failed";
    
    // 反序列化
    std::ifstream in(tempFile, std::ios::binary);
    ASSERT_TRUE(in.good()) << "Failed to open serialized file";
    
    auto restored = HashTable<true>::deserialize(in, pool_.get());
    in.close();
    
    // 验证数据
    EXPECT_EQ(restored->numDistinct(), 5);
    EXPECT_EQ(restored->rows()->numRows(), 5);
    
    // 清理
    std::remove(tempFile.c_str());
  }
}

// ============================================================================
// 多进程并发序列化测试（模拟分布式环境）
// ============================================================================

TEST_F(HashTableSerializationTest, MultiProcessConcurrentSerialization) {
  const int numProcesses = 4;
  std::vector<std::string> tempFiles;
  
  // 创建多个子进程，每个序列化不同的数据
  for (int i = 0; i < numProcesses; ++i) {
    std::string tempFile = tempDir_->getPath() + 
                          "/hashtable_process_" + std::to_string(i) + ".bin";
    tempFiles.push_back(tempFile);
    
    pid_t pid = fork();
    
    if (pid == 0) {
      // 子进程
      auto childPool = memory::memoryManager()->addLeafPool();
      auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
      
      // 每个进程创建不同范围的数据
      int start = i * 1000;
      int end = start + 1000;
      
      std::vector<int64_t> keys;
      std::vector<std::string> values;
      for (int j = start; j < end; ++j) {
        keys.push_back(j);
        values.push_back("process_" + std::to_string(i) + "_value_" + std::to_string(j));
      }
      
      auto data = makeRowVector({
          makeFlatVector(keys),
          makeFlatVector(values)
      });
      
      insertData(table.get(), data, {0});
      table->prepareJoinTable(
          {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
      
      // 序列化
      std::ofstream out(tempFile, std::ios::binary);
      table->serialize(out);
      out.close();
      
      exit(0);
    }
  }
  
  // 父进程：等待所有子进程完成
  for (int i = 0; i < numProcesses; ++i) {
    int status;
    wait(&status);
    ASSERT_EQ(WEXITSTATUS(status), 0) << "Child process " << i << " failed";
  }
  
  // 验证所有序列化的文件
  for (int i = 0; i < numProcesses; ++i) {
    std::ifstream in(tempFiles[i], std::ios::binary);
    ASSERT_TRUE(in.good()) << "Failed to open file from process " << i;
    
    auto restored = HashTable<true>::deserialize(in, pool_.get());
    in.close();
    
    EXPECT_EQ(restored->numDistinct(), 1000) 
        << "Process " << i << " data mismatch";
    
    // 清理
    std::remove(tempFiles[i].c_str());
  }
}

// ============================================================================
// 跨进程数据合并测试（模拟 Shuffle）
// ============================================================================

TEST_F(HashTableSerializationTest, CrossProcessDataMerge) {
  const int numPartitions = 3;
  std::vector<std::string> partitionFiles;
  
  // 阶段1：多个进程创建分区数据
  for (int i = 0; i < numPartitions; ++i) {
    std::string tempFile = tempDir_->getPath() + 
                          "/partition_" + std::to_string(i) + ".bin";
    partitionFiles.push_back(tempFile);
    
    pid_t pid = fork();
    
    if (pid == 0) {
      // 子进程创建分区数据
      auto childPool = memory::memoryManager()->addLeafPool();
      auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
      
      // 模拟哈希分区：只包含 hash(key) % numPartitions == i 的数据
      std::vector<int64_t> keys;
      std::vector<std::string> values;
      
      for (int64_t key = 0; key < 1000; ++key) {
        if (key % numPartitions == i) {
          keys.push_back(key);
          values.push_back("partition_" + std::to_string(i) + "_key_" + std::to_string(key));
        }
      }
      
      auto data = makeRowVector({
          makeFlatVector(keys),
          makeFlatVector(values)
      });
      
      insertData(table.get(), data, {0});
      table->prepareJoinTable(
          {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
      
      std::ofstream out(tempFile, std::ios::binary);
      table->serialize(out);
      out.close();
      
      exit(0);
    }
  }
  
  // 等待所有分区创建完成
  for (int i = 0; i < numPartitions; ++i) {
    int status;
    wait(&status);
    ASSERT_EQ(WEXITSTATUS(status), 0);
  }
  
  // 阶段2：父进程合并所有分区
  auto mergedTable = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  int totalRows = 0;
  for (int i = 0; i < numPartitions; ++i) {
    std::ifstream in(partitionFiles[i], std::ios::binary);
    ASSERT_TRUE(in.good());
    
    auto partition = HashTable<true>::deserialize(in, pool_.get());
    in.close();
    
    totalRows += partition->numDistinct();
    
    // 清理
    std::remove(partitionFiles[i].c_str());
  }
  
  // 验证总行数
  EXPECT_EQ(totalRows, 1000) << "Merged data count mismatch";
}

// ============================================================================
// 性能基准测试
// ============================================================================

TEST_F(HashTableSerializationTest, PerformanceBenchmark) {
  // 创建大规模数据集
  const int numRows = 100000;
  
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  std::vector<int64_t> keys;
  std::vector<std::string> values;
  for (int i = 0; i < numRows; ++i) {
    keys.push_back(i);
    values.push_back("benchmark_value_" + std::to_string(i));
  }
  
  auto data = makeRowVector({
      makeFlatVector(keys),
      makeFlatVector(values)
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  // 测试默认序列化路径性能
  {
    auto start = std::chrono::high_resolution_clock::now();
    
    std::stringstream ss;
    table->serialize(ss);
    
    auto serEnd = std::chrono::high_resolution_clock::now();
    
    auto restored = HashTable<true>::deserialize(ss, pool_.get());
    
    auto deserEnd = std::chrono::high_resolution_clock::now();
    
    auto serTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        serEnd - start).count();
    auto deserTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        deserEnd - serEnd).count();
    
    LOG(INFO) << "Default Serialization Performance (" << numRows
              << " rows):";
    LOG(INFO) << "  Serialization: " << serTime << " ms";
    LOG(INFO) << "  Deserialization: " << deserTime << " ms";
    LOG(INFO) << "  Total: " << (serTime + deserTime) << " ms";
  }
  
}

// ============================================================================
// 错误处理测试
// ============================================================================

TEST_F(HashTableSerializationTest, InvalidMagicNumber) {
  std::stringstream ss;
  uint32_t invalidMagic = 0x12345678;
  ss.write(reinterpret_cast<const char*>(&invalidMagic), sizeof(invalidMagic));
  
  EXPECT_THROW(
      HashTable<true>::deserialize(ss, pool_.get()),
      VeloxException);
}

TEST_F(HashTableSerializationTest, UnsupportedVersion) {
  std::stringstream ss;
  uint32_t magic = 0x48415348;
  uint32_t version = 999;
  ss.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  ss.write(reinterpret_cast<const char*>(&version), sizeof(version));
  
  EXPECT_THROW(
      HashTable<true>::deserialize(ss, pool_.get()),
      VeloxException);
}

TEST_F(HashTableSerializationTest, CorruptedData) {
  auto table = createTestHashTable({BIGINT()}, {VARCHAR()}, false);
  
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"})
  });
  
  insertData(table.get(), data, {0});
  table->prepareJoinTable(
      {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
  
  std::stringstream ss;
  table->serialize(ss);
  
  // 截断序列化流，确保反序列化会在读取过程中遇到不完整输入。
  std::string serialized = ss.str();
  ASSERT_GT(serialized.size(), 8);
  serialized.resize(serialized.size() - 8);

  std::stringstream corruptedSs(serialized);

  EXPECT_THROW(
      HashTable<true>::deserialize(corruptedSs, pool_.get()),
      VeloxException);
}

} // namespace facebook::velox::exec::test

// Made with Bob
