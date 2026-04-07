LIB_DIR ?= /usr/local/lib
INCLUDE_DIR ?= .

CFLAGS = -Wall -Wextra -O2
LDFLAGS = -L$(LIB_DIR) -lrustdfs_client -Wl,-rpath,$(LIB_DIR)

client: client.c rustdfs.h
	gcc $(CFLAGS) -I$(INCLUDE_DIR) -o $@ $< $(LDFLAGS)

clean:
	rm -f client

.PHONY: clean
