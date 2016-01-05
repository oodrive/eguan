/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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
 * @brief StatusCode management/source
 * @author j. caba
 */
#include "StatusCode.h"
#include <assert.h>

namespace ibs {

StatusCode::StatusCode(Code code) :
        internalCode(code) {
}

StatusCode StatusCode::OK() {
    return StatusCode();
}

StatusCode StatusCode::KeyNotFound() {
    return StatusCode(K_KEY_NOT_FOUND);
}

StatusCode StatusCode::Corruption() {
    return StatusCode(K_CORRUPTION);
}

StatusCode StatusCode::NotSupported() {
    return StatusCode(K_NOT_SUPPORTED);
}

StatusCode StatusCode::InvalidArgument() {
    return StatusCode(K_INVALID_ARGUMENT);
}

StatusCode StatusCode::IOError() {
    return StatusCode(K_IO_ERROR);
}

StatusCode StatusCode::ConfigError() {
    return StatusCode(K_CONFIG_ERROR);
}

StatusCode StatusCode::CreateInExistingIBS() {
    return StatusCode(K_CREATE_IN_EXISTING);
}

StatusCode StatusCode::InitInEmptyDirectory() {
    return StatusCode(K_INIT_IN_EMPTY);
}

StatusCode StatusCode::SliceTooSmall() {
    return StatusCode(K_SLICE_TOO_SMALL);
}

StatusCode StatusCode::KeyAreadyAdded() {
    return StatusCode(K_KEY_ALREADY_ADDED);
}

StatusCode StatusCode::TransactionNotFound() {
    return StatusCode(K_TRANSACTION_NOT_FOUND);
}

bool StatusCode::ok() const {
    return internalCode == K_OK;
}

bool StatusCode::IsKeyNotFound() const {
    return internalCode == K_KEY_NOT_FOUND;
}

bool StatusCode::IsKeyFound() const {
    return ok() || IsSliceTooSmall() || IsKeyAlreadyAdded();
}

bool StatusCode::IsCorruption() const {
    return internalCode == K_CORRUPTION;
}

bool StatusCode::IsIOError() const {
    return internalCode == K_IO_ERROR;
}

bool StatusCode::IsConfigError() const {
    return internalCode == K_CONFIG_ERROR;
}

bool StatusCode::IsCreateInExistingIBS() const {
    return internalCode == K_CREATE_IN_EXISTING;
}

bool StatusCode::IsInitInEmptyDirectory() const {
    return internalCode == K_INIT_IN_EMPTY;
}

bool StatusCode::IsNotSupported() const {
    return internalCode == K_NOT_SUPPORTED;
}

bool StatusCode::IsSliceTooSmall() const {
    return internalCode == K_SLICE_TOO_SMALL;
}

bool StatusCode::IsKeyAlreadyAdded() const {
    return internalCode == K_KEY_ALREADY_ADDED;
}

bool StatusCode::IsTransactionNotFound() const {
    return internalCode == K_TRANSACTION_NOT_FOUND;
}

std::string StatusCode::ToString() const {
    switch (internalCode) {
        case K_OK:
            return "OK";
        case K_KEY_NOT_FOUND:
            return "Key not found";
        case K_CORRUPTION:
            return "Corruption";
        case K_NOT_SUPPORTED:
            return "Not implemented";
        case K_INVALID_ARGUMENT:
            return "Invalid argument";
        case K_IO_ERROR:
            return "IO error";
        case K_SLICE_TOO_SMALL:
            return "Slice too small";
        case K_KEY_ALREADY_ADDED:
            return "Key already added";
        case K_TRANSACTION_NOT_FOUND:
            return "Transaction not found";
        case K_CONFIG_ERROR:
            return "Config error";
        case K_CREATE_IN_EXISTING:
            return "Create in existing";
        case K_INIT_IN_EMPTY:
            return "Init in empty";
        case K_DUMP_ERROR:
            return "Atomic write error";
        case K_UNKOWN:
        default:
            return "Unknown code";
    }
}

uint64_t StatusCode::getCode() const {
    return internalCode;
}

} /* namespace ibs */
