/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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
#include <stdint.h>
typedef uint64_t word64;
typedef uint32_t word32;
typedef uint8_t byte;

/* Big endian:                                         */
#if !(defined(__alpha)||defined(__i386__)||defined(__vax__)||defined(__x86_64__))
#define BIG_ENDIAN
#endif

/* The following macro denotes that an optimization    */
/* for Alpha is required. It is used only for          */
/* optimization of time. Otherwise it does nothing.    */
#if (defined(__alpha) || defined(__x86_64__))
#define OPTIMIZE_FOR_ALPHA
#endif

/* NOTE that this code is NOT FULLY OPTIMIZED for any  */
/* machine. Assembly code might be much faster on some */
/* machines, especially if the code is compiled with   */
/* gcc.                                                */

/* The number of passes of the hash function.          */
/* Three passes are recommended.                       */
/* Use four passes when you need extra security.       */
/* Must be at least three.                             */
#define PASSES 3

extern word64 table[4*256];

#define t1 (table)
#define t2 (table+256)
#define t3 (table+256*2)
#define t4 (table+256*3)

#define save_abc \
      aa = a; \
      bb = b; \
      cc = c;

#ifdef OPTIMIZE_FOR_ALPHA
/* This is the official definition of round */
#define round(a,b,c,x,mul) \
      c ^= x; \
      a -= t1[((c)>>(0*8))&0xFF] ^ t2[((c)>>(2*8))&0xFF] ^ \
	   t3[((c)>>(4*8))&0xFF] ^ t4[((c)>>(6*8))&0xFF] ; \
      b += t4[((c)>>(1*8))&0xFF] ^ t3[((c)>>(3*8))&0xFF] ^ \
	   t2[((c)>>(5*8))&0xFF] ^ t1[((c)>>(7*8))&0xFF] ; \
      b *= mul;
#else
/* This code works faster when compiled on 32-bit machines */
/* (but works slower on Alpha) */
#define round(a,b,c,x,mul) \
      c ^= x; \
      a -= t1[(byte)(c)] ^ \
           t2[(byte)(((word32)(c))>>(2*8))] ^ \
	   t3[(byte)((c)>>(4*8))] ^ \
           t4[(byte)(((word32)((c)>>(4*8)))>>(2*8))] ; \
      b += t4[(byte)(((word32)(c))>>(1*8))] ^ \
           t3[(byte)(((word32)(c))>>(3*8))] ^ \
	   t2[(byte)(((word32)((c)>>(4*8)))>>(1*8))] ^ \
           t1[(byte)(((word32)((c)>>(4*8)))>>(3*8))]; \
      b *= mul;
#endif

#define pass(a,b,c,mul) \
      round(a,b,c,x0,mul) \
      round(b,c,a,x1,mul) \
      round(c,a,b,x2,mul) \
      round(a,b,c,x3,mul) \
      round(b,c,a,x4,mul) \
      round(c,a,b,x5,mul) \
      round(a,b,c,x6,mul) \
      round(b,c,a,x7,mul)

#define key_schedule \
      x0 -= x7 ^ 0xA5A5A5A5A5A5A5A5LL; \
      x1 ^= x0; \
      x2 += x1; \
      x3 -= x2 ^ ((~x1)<<19); \
      x4 ^= x3; \
      x5 += x4; \
      x6 -= x5 ^ ((~x4)>>23); \
      x7 ^= x6; \
      x0 += x7; \
      x1 -= x0 ^ ((~x7)<<19); \
      x2 ^= x1; \
      x3 += x2; \
      x4 -= x3 ^ ((~x2)>>23); \
      x5 ^= x4; \
      x6 += x5; \
      x7 -= x6 ^ 0x0123456789ABCDEFLL;

#define feedforward \
      a ^= aa; \
      b -= bb; \
      c += cc;

#ifdef OPTIMIZE_FOR_ALPHA
/* The loop is unrolled: works better on Alpha */
#define compress \
      save_abc \
      pass(a,b,c,5) \
      key_schedule \
      pass(c,a,b,7) \
      key_schedule \
      pass(b,c,a,9) \
      for(pass_no=3; pass_no<PASSES; pass_no++) { \
        key_schedule \
	pass(a,b,c,9) \
	tmpa=a; a=c; c=b; b=tmpa;} \
      feedforward
#else
/* loop: works better on PC and Sun (smaller cache?) */
#define compress \
      save_abc \
      for(pass_no=0; pass_no<PASSES; pass_no++) { \
        if(pass_no != 0) {key_schedule} \
	pass(a,b,c,(pass_no==0?5:pass_no==1?7:9)); \
	tmpa=a; a=c; c=b; b=tmpa;} \
      feedforward
#endif

#define tiger_compress_macro(str, state) \
{ \
  register word64 a, b, c, tmpa; \
  word64 aa, bb, cc; \
  register word64 x0, x1, x2, x3, x4, x5, x6, x7; \
  register word32 i; \
  int pass_no; \
\
  a = state[0]; \
  b = state[1]; \
  c = state[2]; \
\
  x0=str[0]; x1=str[1]; x2=str[2]; x3=str[3]; \
  x4=str[4]; x5=str[5]; x6=str[6]; x7=str[7]; \
\
  compress; \
\
  state[0] = a; \
  state[1] = b; \
  state[2] = c; \
}

#ifdef OPTIMIZE_FOR_ALPHA
/* The compress function is inlined: works better on Alpha.        */
/* Still leaves the function above in the code, in case some other */
/* module calls it directly.                                       */
#define tiger_compress(str, state) \
  tiger_compress_macro(((word64*)str), ((word64*)state))

#else
/* The compress function is a function. Requires smaller cache?    */
tiger_compress(word64 *str, word64 state[3])
{
  tiger_compress_macro(((word64*)str), ((word64*)state));
}

#endif

static void tiger(word64 *str, word32 length, word64 res[3])
{
  register word32 i, j;
  unsigned char temp[64];

  res[0]=0x0123456789ABCDEFLL;
  res[1]=0xFEDCBA9876543210LL;
  res[2]=0xF096A5B4C3B2E187LL;

  for(i=length; i>=64; i-=64)
    {
#ifdef BIG_ENDIAN
      for(j=0; j<64; j++)
	temp[j^7] = ((byte*)str)[j];
      tiger_compress(((word64*)temp), res);
#else
      tiger_compress(str, res);
#endif
      str += 8;
    }

#ifdef BIG_ENDIAN
  for(j=0; j<i; j++)
    temp[j^7] = ((byte*)str)[j];

  temp[j^7] = 0x01;
  j++;
  for(; j&7; j++)
    temp[j^7] = 0;
#else
  for(j=0; j<i; j++)
    temp[j] = ((byte*)str)[j];

  temp[j++] = 0x01;
  for(; j&7; j++)
    temp[j] = 0;
#endif
  if(j>56)
    {
      for(; j<64; j++)
	temp[j] = 0;
      tiger_compress(((word64*)temp), res);
      j=0;
    }

  for(; j<56; j++)
    temp[j] = 0;
  ((word64*)(&(temp[56])))[0] = ((word64)length)<<3;
  tiger_compress(((word64*)temp), res);
}

/* +------------------+ */
/* | Java Native code | */
/* +------------------+ */

#include <jni.h>

/* ByteString support */
extern jbyteArray getByteStrArray(JNIEnv *env, jobject data);

/* Status returned by the computation */
#define OK                              0
#define ERR_DIRECT_BUFFER_UNSUPPORTED   1
#define ERR_EXCEPTION                   2

/*
 * Class:     io_eguan_hash_TigerNative
 * Method:    tigerNative
 * Signature: (Ljava/nio/ByteBuffer;IILjava/nio/ByteBuffer;I)I
 */
JNIEXPORT int JNICALL Java_io_eguan_hash_TigerNative_tigerNativeDirect(JNIEnv *env , const jclass clazz,
        const jobject src, const jint offsetSrc, const jint lengthSrc, const jobject dst, const jint offsetDst) {
    /* Ignored parameter */
    (void) clazz;

    void *srcArray;
    void *dstArray;

    /* Get dst bytes: must be a direct buffer */
    dstArray = (*env)->GetDirectBufferAddress(env, dst);
    if (dstArray == NULL) {
        /* Access to this direct buffer not supported */
        return ERR_DIRECT_BUFFER_UNSUPPORTED;
    }

    srcArray = (*env)->GetDirectBufferAddress(env, src);
    if (srcArray == NULL) {
        /* Access to this direct buffer not supported */
        return ERR_DIRECT_BUFFER_UNSUPPORTED;
    }

    tiger((word64*)(((jbyte*)srcArray) + offsetSrc), (word32)lengthSrc, (word64*)(((jbyte*)dstArray) + offsetDst));

    return OK;
}

/*
 * Class:     io_eguan_hash_TigerNative
 * Method:    tigerNative
 * Signature: ([BIILjava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_hash_TigerNative_tigerNative(JNIEnv *env, const jclass clazz,
        const jbyteArray src, const jint offsetSrc, const jint lengthSrc, const jobject dst, const jint offsetDst) {
    /* Ignored parameter */
    (void) clazz;

    void *srcArray;
    void *dstArray;
    jboolean isCopy;

    /* Get dst bytes: must be a direct buffer */
    dstArray = (*env)->GetDirectBufferAddress(env, dst);
    if (dstArray == NULL) {
        /* Access to this direct buffer not supported */
        return ERR_DIRECT_BUFFER_UNSUPPORTED;
    }

    srcArray = (*env)->GetByteArrayElements(env, src, &isCopy);
    if (srcArray == NULL) {
        /* Exception thrown */
        return ERR_EXCEPTION;
    }

    tiger((word64*) (((jbyte*) srcArray) + offsetSrc), (word32)lengthSrc, (word64*) (((jbyte*) dstArray) + offsetDst));

    /* Release mem (unmodified) */
    (*env)->ReleaseByteArrayElements(env, src, srcArray, JNI_ABORT);

    return OK;
}

/*
 * Class:     io_eguan_hash_TigerNative
 * Method:    tigerNativeByteStr
 * Signature: (Lcom/google/protobuf/ByteString;Ljava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_io_eguan_hash_TigerNative_tigerNativeByteStr(JNIEnv *env, const jclass clazz,
        const jobject src, const jobject dst, const jint offsetDst) {
    jbyteArray srcArray;

    /* Ignored parameter */
    (void) clazz;

    srcArray = getByteStrArray(env, src);
    if (srcArray == NULL) {
        /* Unexpected value */
        return ERR_EXCEPTION;
    }
    return Java_io_eguan_hash_TigerNative_tigerNative(env, clazz, srcArray, 0,
            (*env)->GetArrayLength(env, srcArray), dst, offsetDst);
}
