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
 * @file StatusCode.h
 * @brief Status code management/header
 * @author j. caba
 */
#ifndef STATUSCODE_H_
#define STATUSCODE_H_

#include <string>

namespace ibs {

/**
 * @brief StatusCode class. A StatusCode encapsulates the result of an operation.
 *
 * It may indicate success, or it may indicate an error with an associated error message.
 */
class StatusCode {
    public:
        /**
         * @brief Status code definition
         */
        enum Code {
            K_OK = 0,
            K_KEY_NOT_FOUND = 1,
            K_CORRUPTION = 2,
            K_NOT_SUPPORTED = 3,
            K_INVALID_ARGUMENT = 4,
            K_IO_ERROR = 5,
            K_UNKOWN = 6,
            K_CONFIG_ERROR = 7,
            K_CREATE_IN_EXISTING = 8,
            K_INIT_IN_EMPTY = 9,
            K_SLICE_TOO_SMALL = 10,
            K_KEY_ALREADY_ADDED = 11,
            K_TRANSACTION_NOT_FOUND = 12,
            K_DUMP_ERROR = 13
        };

        /**
         * @brief Create a success status.
         */
        StatusCode(Code code = K_OK);

        /**
         * @brief Return a success status.
         * @return A success status.
         */
        static StatusCode OK();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "notFound"
         */
        static StatusCode KeyNotFound();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "corruption"
         */
        static StatusCode Corruption();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "notSupported"
         */
        static StatusCode NotSupported();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "invalidArgument"
         */
        static StatusCode InvalidArgument();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "IOerror"
         */
        static StatusCode IOError();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "ConfigError"
         */
        static StatusCode ConfigError();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "CreateInExistingIBS"
         */
        static StatusCode CreateInExistingIBS();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "InitInEmptyDirectory"
         */
        static StatusCode InitInEmptyDirectory();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "SliceTooSmall"
         */
        static StatusCode SliceTooSmall();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "KeyAreadyAdded"
         */
        static StatusCode KeyAreadyAdded();

        /**
         * @brief Return error status of an appropriate type.
         * @param msg A message that will be include inside the status.
         * @return A status "TransactionNotFound"
         */
        static StatusCode TransactionNotFound();

        /**
         * @brief Returns true if the status indicates success.
         * @return A boolean true = ok / false = ko.
         */
        bool ok() const;

        /**
         * @brief Returns true if the status indicates a NotFound error.
         * @return A boolean true = Not found / false = we do not found the record 
         * (that does not mean that the record does not exist).
         * @Remark IsKeyNotFound is not the negation of IsKeyFound.
         */
        bool IsKeyNotFound() const;

        /**
         * @brief Returns true if the status indicates that a record was found 
         * ie: OK or SliceTooSmall
         * @return A boolean true = Found / false = we do not found the record 
         * (that does not mean that the record does not exist).
         * @Remark IsFound is not the negation of IsNotFound.
         */
        bool IsKeyFound() const;

        /**
         * @brief Returns true if the status indicates a Corruption error.
         * @return A boolean true = Corruption, false = no Corruption
         */
        bool IsCorruption() const;

        /**
         * @brief Returns true if the status indicates an I/O error.
         * @return A boolean true = I/O error, false = no I/O error
         */
        bool IsIOError() const;

        /**
         * @brief Returns true if the status indicates a non-consistent configuration
         * @return A boolean true = ConfigError error, false = no ConfigError error
         */
        bool IsConfigError() const;

        /**
         * @brief Returns true if the status indicates a creation into an existing IBS
         * @return A boolean true = CreateInExistingIBS error, false = no CreateInExistingIBS error
         */
        bool IsCreateInExistingIBS() const;

        /**
         * @brief Returns true if the status indicates an initialization into an empty directory
         * @return A boolean true = InitInEmptyDirectory error, false = no InitInEmptyDirectory error
         */
        bool IsInitInEmptyDirectory() const;

        /**
         * @brief Returns true if the status indicates an unsupported error
         * @return A boolean true = NotSupported error, false = no NotSupported error
         */
        bool IsNotSupported() const;

        /**
         * @brief Returns true if the status indicates a slice too small
         * @return A boolean true = SliceTooSmall error, false = no SliceTooSmall error
         */
        bool IsSliceTooSmall() const;

        /**
         * @brief Returns true if the status indicates an already added key.
         * @return A boolean true = KeyAlreadyAdded error, false = no KeyAlreadyAdded error
         */
        bool IsKeyAlreadyAdded() const;

        /**
         * @brief Returns true if the status indicates a transaction not found.
         * @return A boolean true = TransactionNotFound error, false = no TransactionNotFound error
         */
        bool IsTransactionNotFound() const;

        /**
         * @brief Return a string representation of this status suitable for printing.
         * Returns the string "OK" for success.
         * @return
         */
        std::string ToString() const;

        /**
         * @brief Code getter as unsigned integer
         */
        uint64_t getCode() const;

    private:
        Code internalCode;
};

} /* namespace ibs */
#endif /* STATUSCODE_H_ */
