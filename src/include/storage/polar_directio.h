#ifndef POLAR_DIRECTIO_H
#define POLAR_DIRECTIO_H

#define POLAR_DIRECTIO_MIN_IOSIZE       (4 * 1024)
#define POLAR_DIRECTIO_DEFAULT_IOSIZE   (1 * 1024 * 1024)
#define POLAR_DIRECTIO_MAX_IOSIZE       (128 * 1024 * 1024)

extern int polar_max_direct_io_size;
extern char *polar_directio_buffer;

#define POLAR_ACCESS_MODE_MASK      0x3
#define POLAR_DIRECTIO_ALIGN_LEN		(4 * 1024)
#define POLAR_DIRECTIO_ALIGN_DOWN(LEN)  TYPEALIGN_DOWN(POLAR_DIRECTIO_ALIGN_LEN, LEN)
#define POLAR_DIRECTIO_ALIGN(LEN)       TYPEALIGN(POLAR_DIRECTIO_ALIGN_LEN, LEN)
#define POLAR_DIECRTIO_IS_ALIGNED(LEN)  !((uintptr_t)(LEN) & (uintptr_t)(POLAR_DIRECTIO_ALIGN_LEN - 1))

extern int polar_directio_open(const char *path, int flags, mode_t mode);
extern ssize_t polar_directio_read(int fd, void *buf, size_t len);
extern ssize_t polar_directio_pread(int fd, void *buffer, size_t len, off_t offset);
extern ssize_t polar_directio_write(int fd, const void *buf, size_t len);
extern ssize_t polar_directio_pwrite(int fd, const void *buffer, size_t len, off_t offset);

#endif                          /* POLAR_DIRECTIO_H */
