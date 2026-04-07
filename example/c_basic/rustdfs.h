#ifndef RUSTDFS_H
#define RUSTDFS_H

#include <stdint.h>
#include <stddef.h>

/* Open flags — same semantics as libhdfs / POSIX */
#define RDFS_O_RDONLY  0x0001
#define RDFS_O_WRONLY  0x0002

/* Opaque handles */
typedef struct rdfs_client rdfs_client_t;
typedef struct rdfs_file   rdfs_file_t;

/* POSIX-style return type for read/write byte counts */
typedef int32_t rdfs_size_t;

/*
 * Connect to the name node.
 * Returns 0 on success, -1 on error.
 */
int rdfs_connect(const char *host, uint16_t port, rdfs_client_t **fs_out);

/*
 * Disconnect and free the filesystem handle.
 * Returns 0 on success, -1 on error.
 * The handle is freed even on error.
 */
int rdfs_disconnect(rdfs_client_t *fs);

/*
 * Open a file for reading or writing.
 *
 * flags: RDFS_O_RDONLY or RDFS_O_WRONLY
 *
 * Returns 0 on success, -1 on error.
 */
int rdfs_open(rdfs_client_t *fs, const char *path, int flags,
              rdfs_file_t **file_out);

/*
 * Read data from an open file.
 * Returns bytes read on success, 0 on EOF, -1 on error.
 */
rdfs_size_t rdfs_read(rdfs_client_t *fs, rdfs_file_t *file,
                      void *buffer, rdfs_size_t length);

/*
 * Write data to an open file.
 * Returns bytes written on success, -1 on error.
 */
rdfs_size_t rdfs_write(rdfs_client_t *fs, rdfs_file_t *file,
                       const void *buffer, rdfs_size_t length);

/*
 * Flush buffered write data.
 * Returns 0 on success, -1 on error.
 */
int rdfs_flush(rdfs_client_t *fs, rdfs_file_t *file);

/*
 * Close an open file.
 * For writes: finalizes the write and releases the lease.
 * The file handle is freed even on error.
 * Returns 0 on success, -1 on error.
 */
int rdfs_close(rdfs_client_t *fs, rdfs_file_t *file);

/*
 * Get a human-readable error string for the last failed operation.
 * The returned pointer is valid until the next rdfs_* call on the
 * same thread.
 *
 * Returns NULL if no error information is available.
 */
const char *rdfs_get_last_error(void);

#endif /* RUSTDFS_H */
