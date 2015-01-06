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
 * @file FileTools.h
 * @brief various file tools for Ibs/header
 */
#ifndef FILETOOLS_H_
#define FILETOOLS_H_

#include <string>
#include <vector>
#include <fstream>
#include <iostream>

namespace ibs {

class FileTools {
    public:
        enum Filter {
            nofilter, not_a_directory, directory
        };

        /**
         * @brief Checks if a path exists into the filesystem
         * @param path Path to check
         * @return true if the path exists, false otherwise
         */
        static bool exists(const std::string& path);

        /**
         * @brief Return the basename of the path
         * @param path
         * @return The basename
         */
        static std::string getBasename(const std::string& path);

        /**
         * @brief Match that extension of file is ".ibs"
         * @param fileBasename
         * @return True if ends with ".ibs"
         */
        static bool hasIbsExtension(const std::string& fileBasename);

        /**
         * @brief Return the absolute path of the path
         * @param path
         * @return The absolute path
         */
        static std::string getAbsolutePath(const std::string& inputPath);

        /**
         * @brief Checks if a path exists and is a directory
         * @param path Path to check
         * @return true if the path points to an existing directory, false otherwise
         */
        static bool isDirectory(const std::string& path);

        /**
         * @brief Checks if a path exists, is a directory with sufficient permissions.
         * @param path Path to check
         * @return true if the path points to an existing directory, false otherwise
         */
        static bool dirOk(const std::string& path);

        /**
         * @brief Checks if a path exists and is a file
         * @param path Path to check
         * @return true if the path points to an existing file, false otherwise
         */
        static bool isFile(const std::string& path);

        /**
         * @brief Checks if the user has read permission on the path
         * @param path Path to check
         * @return true if the path points to an existing file, false otherwise
         */
        static bool isReadable(const std::string& path);

        /**
         * @brief Checks if the user has write permission on the path
         * @param path Path to check
         * @return true if the path points to an existing file, false otherwise
         */
        static bool isWritable(const std::string& path);

        /**
         * @brief Lists the content of the path if it is an existing and readable directory
         * @param iPath Path to the folder to be listed
         * @param oFilenames A vector to be filled with the filenames. This vector does not contain '.' nor '..'
         * @return true if the path is a readable directory, false otherwise
         */
        static bool list(const std::string& iPath, std::vector<std::string>& oFilenames, Filter iFilter = nofilter);

        /**
         * @brief Check if a directory is empty or not.
         * @param dirname
         * @return true if the directory is empty, false otherwise
         */
        static bool isDirectoryEmpty(const std::string& path);

        /**
         * @brief
         * @param path
         * @return True if the directory was created or is created.
         */
        static bool createDirectory(const std::string& path);

        /**
         * @brief Recursive remove of a directory
         * @param path
         */
        static void removeDirectory(const std::string& path);

        /**
         * @brief Remove of an empty directory or file
         * @param path
         */
        static void removeFile(const std::string& path);

    private:

        // non copyable, non constructible
        FileTools() = delete;
        ~FileTools() = delete;
        FileTools(const FileTools&) = delete;
        FileTools& operator=(const FileTools&) = delete;
};

/**
 * @brief Base class for classes using binary files to store data
 */
class BinaryFileBase {
    protected:
        BinaryFileBase() = default;
        virtual ~BinaryFileBase();

        /**
         * @brief Given a file stream in binary mode save a raw byte buffer.
         * @param f file stream as a <code>std::ofstream</code> in binary mode
         * @param input input string as bytes
         * @param inputSize input size (in number of bytes)
         */
        static void setRawBytes(std::ofstream& f, const char* input, const size_t inputSize);

        /**
         * @brief Given a file stream in binary mode save a raw byte buffer.
         * @param f file stream as a <code>std::fstream</code> in binary mode
         * @param input input string as bytes
         * @param inputSize input size (in number of bytes)
         */
        static void updateRawBytes(std::fstream& f, const char* input, const size_t inputSize);

        /**
         * @brief Given a file stream in binary mode encode a
         * string with it's size. The size is first encoded in the file with
         * <code>setUint64</code> at the actual position
         * then the bytes of the string are saved consecutively in the file.
         * @param f file stream as a <code>std::ofstream</code> in binary mode
         * @param input input string as a reference (for performance issue)
         * @see setUint64
         * @see setRawBytes
         */
        static void setString(std::ofstream& f, const std::string& input);

        /**
         * @brief Given a file stream in binary mode encode a
         * string with it's size. The size is first encoded in the file with
         * <code>setUint64</code> at the actual position
         * then the bytes of the string are saved consecutively in the file.
         * @param f file stream as a <code>std::fstream</code> in binary mode
         * @param input input string as a reference (for performance issue)
         * @see updateUint64
         * @see updateRawBytes
         */
        void updateString(std::fstream& f, const std::string& input);

        /**
         * @brief Given a file stream in binary mode return the encoded
         * string with it's size. The size is first decoded from the file with
         * <code>getUint64Encoding</code> at the actual position
         * then the bytes of the string are recovered consecutively in the file.
         * @param f file stream as <code>std::ifstream</code> in binary mode
         * @param output output string as a reference (for performance issue)
         * @see getUint64Encoding
         */
        static void getString(std::ifstream& f, std::string& output);

        /**
         * @brief Given a file stream in binary mode return the encoded
         * string with it's size. The size is first decoded from the file with
         * <code>getUint64Encoding</code> at the actual position
         * then the bytes of the string are recovered consecutively in the file.
         * @param f file stream as <code>std::fstream</code> in binary mode
         * @param output output string as a reference (for performance issue)
         * @see getUint64Encoding
         */
        static void fetchString(std::fstream& f, std::string& output);

        /**
         * @brief Given a file stream in binary mode encode a
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream as <code>std::ofstream</code> in binary mode
         * @param uinteger 64bit unsigned integer
         * @see setUint64Encoding
         */
        static void setUint64(std::ofstream& f, const uint64_t uinteger);

        /**
         * @brief Given a file stream in binary mode encode a
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream as <code>std::fstream</code> in binary mode
         * @param uinteger 64bit unsigned integer
         * @see setUint64Encoding
         */
        static void updateUint64(std::fstream& f, const uint64_t uinteger);

        /**
         * @brief Given a file stream in binary mode return the encoded
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream as <code>std::ifstream</code> in binary mode
         * @return 64bit unsigned integer
         * @see getUint64Encoding
         */
        static uint64_t getUint64(std::ifstream& f);

        /**
         * @brief Given a file stream in binary mode return the encoded
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream as <code>std::fstream</code> in binary mode
         * @return 64bit unsigned integer
         * @see getUint64Encoding
         */
        static uint64_t fetchUint64(std::fstream& f);
    private:

        /**
         * @brief Given a file stream in binary mode return the encoded
         * string with it's size. The size is first decoded from the file with
         * <code>getUint64Encoding</code> at the actual position
         * then the bytes of the string are recovered consecutively in the file.
         * @param f file stream in binary mode
         * @param output output string as a reference (for performance issue)
         * @see getUint64Encoding
         */
        template<typename T>
        static void getStringEncoding(T& f, std::string& output) {
            size_t outputSize = getUint64Encoding(f);
            char* buffer = new char[outputSize];
            for (size_t i = 0; i < outputSize; i++) {
                char fetchedByte;
                f.get(fetchedByte);
                buffer[i] = fetchedByte;
            }
            output.assign(buffer, outputSize);
            delete[] buffer;
        }

        /**
         * @brief Given a file stream in binary mode encode a
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream in binary mode
         * @param uinteger 64bit unsigned integer
         */
        template<typename T>
        static void setUint64Encoding(T& f, const uint64_t uinteger) {
            for (u_short i = 0; i < sizeof(uint64_t); i++) {
                // assume a byte is 8 bits
                u_char storedByte = uinteger >> (i << 3);
                f << storedByte;
            }
        }

        /**
         * @brief Given a file stream in binary mode return the encoded
         * 64bit unsigned integer at the actual position in the file.
         * @param f file stream in binary mode
         * @return 64bit unsigned integer
         */
        template<typename T>
        static uint64_t getUint64Encoding(T& f) {
            uint64_t uinteger = 0;
            for (u_short i = 0; i < sizeof(uint64_t); i++) {
                char fetchedByte;
                f.get(fetchedByte);
                // assume a byte is 8 bits
                uinteger |= static_cast<u_char>(fetchedByte) << (i << 3);
            }
            return uinteger;
        }
};

} /* namespace ibs */
#endif /* FILETOOLS_H_ */
