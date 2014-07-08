/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
#include <jni.h>

/** Fields to get byte array from a ByteString */
static jclass protobufByteStringClass = NULL;
static jfieldID protobufByteStringBytesField = NULL;

/**
 * Gets the array corresponding held by the BytreString.
 * @return the byte array or NULL on failure.
 */
jbyteArray getByteStrArray(JNIEnv *env, jobject data) {

    /* Load IDs if necessary */
    if (protobufByteStringClass == NULL) {
        protobufByteStringClass = (*env)->FindClass(env, "com/google/protobuf/ByteString");
        if (protobufByteStringClass == NULL) {
            /* Exception thrown */
            return NULL;
        }
    }
    if (protobufByteStringBytesField == NULL) {
        protobufByteStringBytesField = (*env)->GetFieldID(env, protobufByteStringClass, "bytes", "[B");
        if (protobufByteStringBytesField == NULL) {
            /* Exception thrown */
            return NULL;
        }
    }

    /* Get object field and delegate call to the put of a byte array */
    return (*env)->GetObjectField(env, data, protobufByteStringBytesField);
}
