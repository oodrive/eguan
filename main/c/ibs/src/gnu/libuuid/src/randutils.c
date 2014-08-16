// Very basic replacement for randutils that is distributed under LGPL
// /dev/urandom is used and it's supposed that the system generates enough entropy
// This file is in the public domain but WITHOUT ANY WARRANTY ...
// No copyright is claimed, do whith it what you wish.
//
// Written by Jean-Manuel CABA <j.caba@oodrive.fr>
//

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

const char* random_tell_source() {
  return "/dev/urandom";
}

int random_get_fd() {
  // initialize seed and do some rand
  sranddev();
  int N = rand() % 11 + 2;
  for(int i=0;i<N;++i){
    rand();
  }
  // finally try to open urandom
  return open(random_tell_source(), O_RDONLY);
}

void random_get_bytes(void* buf, const size_t nbytes) {
  unsigned char* bytes = (unsigned char*) buf;
  const unsigned char* stop = bytes + nbytes;
  const int fd = random_get_fd();

  if (fd >= 0) {
    // read bytes from urandom
    while (bytes < stop) {
      const size_t readed = read(fd, bytes, nbytes);
      bytes += readed;
    }
    close(fd);
  }else{
    // could not open urandom so just use rand
    while (bytes < stop) {
      int prn = rand();
      memcpy(bytes, & prn, sizeof(prn));
      bytes += sizeof(prn);
    }
  }

  return;
}
