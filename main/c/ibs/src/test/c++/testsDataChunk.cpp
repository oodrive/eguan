/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2015 Oodrive
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
/**
 * @file testsDataChunk.h
 * @author j. caba
 */
#include "testsTools.h"
#include "Logger.h"
#include "DataChunk.h"
#include <thread>
#include <atomic>
#include <future>

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("testsDataChunk");

namespace testDataChunk {
const char* threadsBuffer = "Threads";
const int nbThreads = 50;

int makeDataChunks(const int nbLoop, DataChunk& output) {
    int ret = 0;
    for (int i = 0; i < nbLoop; i++) {
        DataChunk chunk(threadsBuffer);
        ret = chunk.toInt();
        EXPECT_TRUE(chunk.isReferencing(threadsBuffer));
        output.reference(chunk);
    }
    return ret;
}

static std::promise<int> promises[nbThreads];
static std::future<int> futures[nbThreads];

DataChunk sharedData;

void detach_threads() {
    const int nbDataChunk = 1000;
    for (int i = 0; i < nbThreads; i++) {
        promises[i] = std::promise<int>();
        futures[i] = promises[i].get_future();
        std::thread([](std::promise<int>& p) {p.set_value(makeDataChunks(nbDataChunk,sharedData));},
                std::ref(promises[i])).detach();
    }
}

void wait_futures() {
    for (int i = 0; i < nbThreads; i++) {
        futures[i].wait();
        ASSERT_TRUE(sharedData.isReferencing(threadsBuffer));
        ASSERT_TRUE(futures[i].get() == sharedData.toInt());
    }
}

class KeyValueNode {
    public:
        KeyValueNode(const DataChunk&& key, const DataChunk&& data) :
                key(std::move(key)), data(std::move(data)) {
        }

        const DataChunk& getKey() {
            return key;
        }
        const DataChunk& getData() {
            return data;
        }
        void setData(const DataChunk& d) {
            data.reference(d);
        }
    private:
        DataChunk key;
        DataChunk data;
};

} // namespace testDataChunk

using namespace testDataChunk;

class testsDataChunk: public ::testing::Test {
    protected:
        static const char* testBuffer;
        static const char* keyBuffer;
        static const char* dataBuffer;
        static char intBuffer[4];

        DataChunk a;
        DataChunk b;
        KeyValueNode m;
        DataChunk intData;

        testsDataChunk() :
                a(testBuffer), b(std::move(a)), m(DataChunk(keyBuffer), DataChunk("data")), intData(intBuffer, 4) {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        }

        virtual ~testsDataChunk() {
        }

        virtual void SetUp() {
            ASSERT_TRUE(sizeof(int) == 4);

        }

        virtual void TearDown() {
        }
};

const char* testsDataChunk::testBuffer = "Test";
const char* testsDataChunk::keyBuffer = "key";
const char* testsDataChunk::dataBuffer = "data";
char testsDataChunk::intBuffer[4] = { 0, 1, 0, 0 }; //256

TEST_F(testsDataChunk, equality) {
    EXPECT_TRUE((a == b) && (b == DataChunk("Test")));
}

TEST_F(testsDataChunk, isReferencing) {
    EXPECT_TRUE(a.isReferencing(testBuffer));
    EXPECT_TRUE(b.isReferencing(testBuffer));
    EXPECT_TRUE(!b.isReferencing(a.toString().data()));
}

TEST_F(testsDataChunk, KeyValueNode) {
    EXPECT_TRUE(m.getKey().toString() == keyBuffer);
    EXPECT_TRUE(m.getData().toString() == dataBuffer);

    m.setData(a);

    EXPECT_TRUE(m.getData().toString() == "Test");
    EXPECT_TRUE(m.getData().isReferencing(testBuffer));
}

#ifdef DEBUG
TEST_F(testsDataChunk, toInt) {
    EXPECT_TRUE(intData.toInt(0) == 1 << 8);

    bool detectedOutOfBoundCopy = false;
    intBuffer[2] = 1;
    try {
        EXPECT_TRUE(intData.toInt(0, true) == ((1 << 8) | (1 << 16)));
    }
    catch (errors::AbortException& e) {
        LOG4IBS_ERROR(logger, "AbortException: Out of bound copy should not be detected!");
        detectedOutOfBoundCopy = true;
    }
    ASSERT_FALSE(detectedOutOfBoundCopy);
    try {
        ibs::errors::ExceptionTracer::disableRedTrace();
        intData.toInt(1, true);
    }
    catch (errors::AbortException& e) {
        ibs::errors::ExceptionTracer::enableRedTrace();
        LOG4IBS_WARN(logger, "AbortException: Out of bound copy detected as expected");
        detectedOutOfBoundCopy = true;
    }
    ibs::errors::ExceptionTracer::enableRedTrace();
    ASSERT_TRUE(detectedOutOfBoundCopy);
}
#endif

TEST_F(testsDataChunk, promiseFutureMultiThread) {
    detach_threads();
    wait_futures();

    EXPECT_TRUE(sharedData.isReferencing(threadsBuffer));
}

TEST_F(testsDataChunk, Compression) {
    string compressible = "AAAABBBBBBCCCCCCDDDDDD1234567890000000000";
    a.reference(compressible.data());
    EXPECT_TRUE(a.isReferencing(compressible.data()));
    EXPECT_TRUE(a.toString() == compressible);

    string compressed = a.toCompressedString();
    EXPECT_TRUE(compressed.size() < compressible.size());

    DataChunk ref_compressed(compressed);
    string uncompressed = ref_compressed.toUncompressedString();
    DataChunk ref_uncompressed(uncompressed);
    EXPECT_TRUE(uncompressed.size() == compressible.size());
    EXPECT_TRUE(uncompressed == compressible);

    EXPECT_FALSE(a.toString() == ref_compressed.toString());
    EXPECT_TRUE(a.toString() == ref_uncompressed.toString());

    EXPECT_TRUE(a.isReferencing(compressible.data()));
    EXPECT_TRUE(a.toString() == compressible);

    EXPECT_FALSE(ref_compressed.copyFrom(uncompressed));
    EXPECT_TRUE(a.copyFrom(uncompressed));

    EXPECT_TRUE(a.isReferencing(compressible.data()));
    EXPECT_TRUE(a.toString() == compressible);
    EXPECT_TRUE(a.hash() == DataChunk(compressible).hash());
}

TEST_F(testsDataChunk, DefaultConstructor) {
    DataChunk empty;
    EXPECT_TRUE(empty.isReferencing(NULL));
    EXPECT_TRUE(empty.getData() == NULL);
    EXPECT_TRUE(empty.getSize() == 0);
    DataChunk emptyRef;
    emptyRef.reference(empty);
    EXPECT_TRUE(emptyRef.isReferencing(NULL));
    EXPECT_TRUE(emptyRef.getData() == NULL);
    EXPECT_TRUE(emptyRef.getSize() == 0);
    EXPECT_TRUE(emptyRef == empty);
}

} /* namespace ibs */

