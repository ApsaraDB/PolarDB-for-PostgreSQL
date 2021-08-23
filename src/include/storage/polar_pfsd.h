#ifndef POLAR_PFSD_H
#define POLAR_PFSD_H


#define PFSD_MIN_MAX_IOSIZE         (4 * 1024)
#define PFSD_DEFAULT_MAX_IOSIZE (4 * 1024 * 1024)
#define PFSD_MAX_MAX_IOSIZE         (128 * 1024 * 1024)

extern  int max_pfsd_io_size;

extern ssize_t polar_pfsd_read(int fd, void *buf, size_t len);
extern ssize_t polar_pfsd_pread(int fd, void *buf, size_t len, off_t offset);
extern ssize_t polar_pfsd_write(int fd, const void *buf, size_t len);
extern ssize_t polar_pfsd_pwrite(int fd, const void *buf, size_t len, off_t offset);

#endif                          /* POLAR_PFSD_H */
