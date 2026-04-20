#include "rustdfs.h"
#include <stdio.h>
#include <stdlib.h>

#define BUF_SIZE (16 * 1024)  /* 16 KB — matches message-size */

int write_file(rdfs_client_t *fs, const char *local, const char *remote) {
    FILE *fp = fopen(local, "rb");
    if (!fp) return -1;

    rdfs_file_t *file;
    if (rdfs_open(fs, remote, RDFS_O_WRONLY, &file) < 0) {
        fprintf(stderr, "open failed: %s\n", rdfs_get_last_error());
        fclose(fp);
        return -1;
    }

    char buf[BUF_SIZE];
    size_t n;
    while ((n = fread(buf, 1, BUF_SIZE, fp)) > 0) {
        if (rdfs_write(fs, file, buf, (rdfs_size_t)n) < 0) {
            fprintf(stderr, "write failed: %s\n", rdfs_get_last_error());
            rdfs_close(fs, file);
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
    return rdfs_close(fs, file);
}

int read_file(rdfs_client_t *fs, const char *remote, const char *local) {
    rdfs_file_t *file;
    if (rdfs_open(fs, remote, RDFS_O_RDONLY, &file) < 0) {
        fprintf(stderr, "open failed: %s\n", rdfs_get_last_error());
        return -1;
    }

    FILE *fp = fopen(local, "wb");
    if (!fp) {
        rdfs_close(fs, file);
        return -1;
    }

    char buf[BUF_SIZE];
    rdfs_size_t n;
    while ((n = rdfs_read(fs, file, buf, BUF_SIZE)) > 0) {
        fwrite(buf, 1, (size_t)n, fp);
    }

    fclose(fp);
    return rdfs_close(fs, file);
}

int main(void) {
    rdfs_client_t *fs;
    if (rdfs_connect("namenode", 5000, &fs) < 0) {
        fprintf(stderr, "connect failed: %s\n", rdfs_get_last_error());
        return 1;
    }

    const char *files[] = {"small.txt", "medium.txt", "large.txt"};
    int passed = 0;

    for (int i = 0; i < 3; i++) {
        char src[256], dst[256];
        snprintf(src, sizeof(src), "/root/files/%s", files[i]);
        snprintf(dst, sizeof(dst), "/root/read/%s", files[i]);

        printf("── Writing %s ──\n", files[i]);
        write_file(fs, src, files[i]);

        printf("── Reading %s ──\n", files[i]);
        read_file(fs, files[i], dst);

        printf("── Comparing %s ──\n", files[i]);
        /* byte-compare src and dst */
        FILE *a = fopen(src, "rb"), *b = fopen(dst, "rb");
        int match = 1;
        if (a && b) {
            int ca, cb;
            while ((ca = fgetc(a)) != EOF && (cb = fgetc(b)) != EOF) {
                if (ca != cb) { match = 0; break; }
            }
            if (match) match = (fgetc(a) == EOF && fgetc(b) == EOF);
        } else {
            match = 0;
        }
        if (a) fclose(a);
        if (b) fclose(b);

        printf("  %s %s\n\n", match ? "OK:" : "FAIL:", files[i]);
        if (match) passed++;
    }

    rdfs_disconnect(fs);

    printf("============================================\n");
    printf(" Results: %d / 3 passed\n", passed);
    printf("============================================\n");
    return (passed == 3) ? 0 : 1;
}
