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
/*
 * Native code of class io_eguan_ibs_IbsLevelDB
 */
#include <jni.h>
#include <libibsc.h>

/*
 * IBS errors specific to native code management.
 */
/** Returned when a direct buffer can not be accessed */
#define ERR_DIRECT_BUFFER_UNSUPPORTED (IBS__UNKNOW_ERROR-1)

/*
 * Called when the library is loaded. Notify the JVM that we need
 * the version 1.4 of the environment.
 */
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *jvm, void *reserved) {
    /* Ignored parameter */
    (void) jvm;
    (void) reserved;

    return JNI_VERSION_1_4;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsCreate
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsCreate(JNIEnv *env, jclass clazz, jstring path) {
    /* Ignored parameter */
    (void) clazz;

    /* Get string chars */
    const char *pathChars = (*env)->GetStringUTFChars(env, path, NULL);
    jint ibsRet;

    if (pathChars == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    ibsRet = ibsCreate(pathChars);

    /* Release String */
    (*env)->ReleaseStringUTFChars(env, path, pathChars);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsInit
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsInit(JNIEnv *env, jclass clazz, jstring path) {
    /* Ignored parameter */
    (void) clazz;

    /* Get string chars */
    const char *pathChars = (*env)->GetStringUTFChars(env, path, NULL);
    jint ibsRet;

    if (pathChars == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    ibsRet = ibsInit(pathChars);

    /* Release String */
    (*env)->ReleaseStringUTFChars(env, path, pathChars);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsStart
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsStart(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameters */
    (void) env;
    (void) clazz;

    return ibsStart(id);
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsStop
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsStop(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameters */
    (void) env;
    (void) clazz;

    return ibsStop(id);
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsDelete
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsDelete(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameters */
    (void) env;
    (void) clazz;

    return ibsDelete(id);
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsDestroy
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsDestroy(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameters */
    (void) env;
    (void) clazz;

    return ibsDestroy(id);
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsIsHotDataEnabled
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsIsHotDataEnabled(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameters */
    (void) env;
    (void) clazz;

    return ibsHotDataEnabled(id);
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsGetDirect
 * Signature: (I[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsGetDirect(JNIEnv *env, jclass clazz, jint id,
        jbyteArray key, jobject data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *keyArray;
    jint keyLen = (*env)->GetArrayLength(env, key);
    void *dataArray;
    jint ibsRet;
    size_t readDataLength = 0; /* reset parameter IN/OUT */

    /* Get key bytes */
    keyArray = (*env)->GetByteArrayElements(env, key, (jboolean*) NULL);
    if (keyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    dataArray = (*env)->GetDirectBufferAddress(env, data);
    if (dataArray == NULL) {
        /* Access to this direct buffer not supported */
        (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);
        return ERR_DIRECT_BUFFER_UNSUPPORTED;
    }

    ibsRet = ibsGet(id, (const char*) keyArray, (size_t) keyLen, (char*) dataArray + offset, (size_t) length,
            &readDataLength);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);

    if (ibsRet < 0) {
        /* Error: return error and length */
        return ibsRet << 24 | ((int) readDataLength);
    }
    else {
        /* Success: return read length */
        return (int) readDataLength;
    }
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsGet
 * Signature: (I[B[BII)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsGet(JNIEnv *env, jclass clazz, jint id, jbyteArray key,
        jbyteArray data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *keyArray;
    jint keyLen = (*env)->GetArrayLength(env, key);
    jbyte *dataArray;
    jint ibsRet;
    jboolean isCopy;
    size_t readDataLength = 0; /* reset parameter IN/OUT */

    /* Get key bytes */
    /* TODO: if isCopy is true, if should be better to call malloc()/ibsGet()/SetByteArrayRegion() */
    keyArray = (*env)->GetByteArrayElements(env, key, (jboolean*) NULL);
    if (keyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    dataArray = (*env)->GetByteArrayElements(env, data, &isCopy);
    if (dataArray == NULL) {
        /* Exception thrown */
        (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);
        return IBS__UNKNOW_ERROR;
    }

    ibsRet = ibsGet(id, (const char*) keyArray, (size_t) keyLen, (char*) dataArray + offset, (size_t) length,
            &readDataLength);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);

    if (ibsRet < 0) {
        /* Error: discard data, return error and length */
        (*env)->ReleaseByteArrayElements(env, data, dataArray, JNI_ABORT);
        return ibsRet << 24 | ((int) readDataLength);
    }
    else {
        /* Success: put back data and return read length */
        (*env)->ReleaseByteArrayElements(env, data, dataArray, 0);
        return (int) readDataLength;
    }
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsDel
 * Signature: (I[B)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsDel(JNIEnv *env, jclass clazz, jint id, jbyteArray key) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *keyArray;
    jint keyLen = (*env)->GetArrayLength(env, key);
    jint ibsRet;

    /* Get key bytes */
    keyArray = (*env)->GetByteArrayElements(env, key, (jboolean*) NULL);
    if (keyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    ibsRet = ibsDel(id, (const char*) keyArray, (size_t) keyLen);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsPutDirect
 * Signature: (II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsPutDirect(JNIEnv *env, jclass clazz, jint id, jint txId,
        jbyteArray key, jobject data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *keyArray;
    jint keyLen = (*env)->GetArrayLength(env, key);
    void *dataPut;
    jint ibsRet;

    /* Get key bytes */
    keyArray = (*env)->GetByteArrayElements(env, key, (jboolean*) NULL);
    if (keyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    if (data == NULL) {
        dataPut = (void *) NULL;
    }
    else {
        const char *dataArray = (*env)->GetDirectBufferAddress(env, data);
        if (dataArray == NULL) {
            /* Access to this direct buffer not supported */
            (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);
            return ERR_DIRECT_BUFFER_UNSUPPORTED;
        }

        dataPut = (void *) (dataArray + offset);
    }

    if (txId == 0)
        ibsRet = ibsPut(id, (const char*) keyArray, (size_t) keyLen, (const char*) dataPut, (size_t) length);
    else
        ibsRet = ibsPutTransaction(id, txId, (const char*) keyArray, (size_t) keyLen, (const char*) dataPut,
                (size_t) length);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsPut
 * Signature: (II[B[BII)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsPut(JNIEnv *env, jclass clazz, jint id, jint txId,
        jbyteArray key, jbyteArray data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *keyArray;
    jint keyLen = (*env)->GetArrayLength(env, key);
    jbyte *dataArray;
    jint ibsRet;

    /* Get key bytes */
    keyArray = (*env)->GetByteArrayElements(env, key, (jboolean*) NULL);
    if (keyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    dataArray = (*env)->GetByteArrayElements(env, data, (jboolean*) NULL);
    if (dataArray == NULL) {
        /* Exception thrown */
        (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);
        return IBS__UNKNOW_ERROR;
    }

    if (txId == 0)
        ibsRet = ibsPut(id, (const char*) keyArray, (size_t) keyLen, (const char*) dataArray + offset, (size_t) length);
    else
        ibsRet = ibsPutTransaction(id, txId, (const char*) keyArray, (size_t) keyLen, (const char*) dataArray + offset,
                (size_t) length);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, data, dataArray, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, key, keyArray, JNI_ABORT);

    return ibsRet;
}

/** Fields to get byte array from a ByteString */
static jclass protobufByteStringClass = NULL;
static jfieldID protobufByteStringBytesField = NULL;

/*
 * Class:     io_eguan_ibs_IbsLevelDB
 * Method:    ibsPutByteStr
 * Signature: (I[BLcom/google/protobuf/ByteString;)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsPutByteStr(JNIEnv *env, jclass clazz, jint id,
        jbyteArray key, jobject data) {
    jbyteArray dataArray;

    /* Ignored parameter */
    (void) clazz;

    if (protobufByteStringClass == NULL) {
        protobufByteStringClass = (*env)->FindClass(env, "com/google/protobuf/ByteString");
        if (protobufByteStringClass == NULL) {
            /* Exception thrown */
            return IBS__UNKNOW_ERROR;
        }
    }
    if (protobufByteStringBytesField == NULL) {
        protobufByteStringBytesField = (*env)->GetFieldID(env, protobufByteStringClass, "bytes", "[B");
        if (protobufByteStringBytesField == NULL) {
            /* Exception thrown */
            return IBS__UNKNOW_ERROR;
        }
    }

    /* Get object field and delegate call to the put of a byte array */
    dataArray = (*env)->GetObjectField(env, data, protobufByteStringBytesField);
    if (dataArray == NULL) {
        /* Unexpected value */
        return IBS__UNKNOW_ERROR;
    }
    return Java_io_eguan_ibs_IbsLevelDB_ibsPut(env, clazz, id, 0, key, dataArray, 0,
            (*env)->GetArrayLength(env, dataArray));
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsReplaceDirect
 * Signature: (II[B[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsReplaceDirect(JNIEnv *env, jclass clazz, jint id,
        jint txId, jbyteArray oldKey, jbyteArray newKey, jobject data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *oldKeyArray;
    jint oldKeyLen = (*env)->GetArrayLength(env, oldKey);
    jbyte *newKeyArray;
    jint newKeyLen = (*env)->GetArrayLength(env, newKey);
    void *dataReplace;
    jint ibsRet;

    /* Get key bytes */
    oldKeyArray = (*env)->GetByteArrayElements(env, oldKey, (jboolean*) NULL);
    if (oldKeyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }
    newKeyArray = (*env)->GetByteArrayElements(env, newKey, (jboolean*) NULL);
    if (newKeyArray == NULL) {
        /* Exception thrown */
        (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    if (data == NULL) {
        dataReplace = (void *) NULL;
    }
    else {
        const char *dataArray = (*env)->GetDirectBufferAddress(env, data);
        if (dataArray == NULL) {
            /* Access to this direct buffer not supported */
            (*env)->ReleaseByteArrayElements(env, newKey, newKeyArray, JNI_ABORT);
            (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);
            return ERR_DIRECT_BUFFER_UNSUPPORTED;
        }

        dataReplace = (void *) (dataArray + offset);
    }

    if (txId == 0)
        ibsRet = ibsReplace(id, (const char*) oldKeyArray, (size_t) oldKeyLen, (const char*) newKeyArray,
                (size_t) newKeyLen, (const char*) dataReplace, (size_t) length);
    else
        ibsRet = ibsReplaceTransaction(id, txId, (const char*) oldKeyArray, (size_t) oldKeyLen,
                (const char*) newKeyArray, (size_t) newKeyLen, (const char*) dataReplace, (size_t) length);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, newKey, newKeyArray, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_Ibs
 * Method:    ibsReplace
 * Signature: (II[B[B[BII)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsReplace(JNIEnv *env, jclass clazz, jint id, jint txId,
        jbyteArray oldKey, jbyteArray newKey, jbyteArray data, jint offset, jint length) {
    /* Ignored parameter */
    (void) clazz;

    jbyte *oldKeyArray;
    jint oldKeyLen = (*env)->GetArrayLength(env, oldKey);
    jbyte *newKeyArray;
    jint newKeyLen = (*env)->GetArrayLength(env, newKey);
    jbyte *dataArray;
    jint ibsRet;

    /* Get key bytes */
    oldKeyArray = (*env)->GetByteArrayElements(env, oldKey, (jboolean*) NULL);
    if (oldKeyArray == NULL) {
        /* Exception thrown */
        return IBS__UNKNOW_ERROR;
    }
    newKeyArray = (*env)->GetByteArrayElements(env, newKey, (jboolean*) NULL);
    if (newKeyArray == NULL) {
        /* Exception thrown */
        (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);
        return IBS__UNKNOW_ERROR;
    }

    /* Get data bytes */
    dataArray = (*env)->GetByteArrayElements(env, data, (jboolean*) NULL);
    if (dataArray == NULL) {
        /* Exception thrown */
        (*env)->ReleaseByteArrayElements(env, newKey, newKeyArray, JNI_ABORT);
        (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);
        return IBS__UNKNOW_ERROR;
    }

    if (txId == 0)
        ibsRet = ibsReplace(id, (const char*) oldKeyArray, (size_t) oldKeyLen, (const char*) newKeyArray,
                (size_t) newKeyLen, (const char*) dataArray + offset, (size_t) length);
    else
        ibsRet = ibsReplaceTransaction(id, txId, (const char*) oldKeyArray, (size_t) oldKeyLen,
                (const char*) newKeyArray, (size_t) newKeyLen, (const char*) dataArray + offset, (size_t) length);

    /* Release mem */
    (*env)->ReleaseByteArrayElements(env, data, dataArray, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, newKey, newKeyArray, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, oldKey, oldKeyArray, JNI_ABORT);

    return ibsRet;
}

/*
 * Class:     io_eguan_ibs_IbsLevelDB
 * Method:    ibsCreateTransaction
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsCreateTransaction(JNIEnv *env, jclass clazz, jint id) {
    /* Ignored parameter */
    (void) env;
    (void) clazz;

    return ibsCreateTransaction(id);
}

/*
 * Class:     io_eguan_ibs_IbsLevelDB
 * Method:    ibsCommitTransaction
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsCommitTransaction(JNIEnv *env, jclass clazz, jint id,
        jint txId) {
    /* Ignored parameter */
    (void) env;
    (void) clazz;

    return ibsCommitTransaction(id, txId);
}

/*
 * Class:     io_eguan_ibs_IbsLevelDB
 * Method:    ibsRollbackTransaction
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_ibs_IbsLevelDB_ibsRollbackTransaction(JNIEnv *env, jclass clazz, jint id,
        jint txId) {
    /* Ignored parameter */
    (void) env;
    (void) clazz;

    return ibsRollbackTransaction(id, txId);
}

