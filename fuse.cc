
#define FUSE_USE_VERSION 31
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fuse.h>
#include <map>
#include <string>
#include <vector>

// In-memory file system structure
struct File {
    std::string name;
    std::string content;
    mode_t mode;
};

std::map<std::string, File> files;

// Get file attributes
static int memfs_getattr(const char* path, struct stat* stbuf,
                         struct fuse_file_info* fi) {
    memset(stbuf, 0, sizeof(struct stat));
    if (std::string(path) == "/") {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    } else if (files.count(path)) {
        stbuf->st_mode = files[path].mode;
        stbuf->st_nlink = 1;
        stbuf->st_size = files[path].content.size();
    } else {
        return -ENOENT;
    }
    return 0;
}

// Read directory
static int memfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info* fi,
                         enum fuse_readdir_flags flags) {
    if (std::string(path) != "/")
        return -ENOENT;

    filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);

    for (const auto& file : files) {
        filler(buf, file.second.name.c_str() + 1, NULL, 0,
               FUSE_FILL_DIR_PLUS); // Skip the leading '/'
    }
    return 0;
}

// Read file
static int memfs_read(const char* path, char* buf, size_t size, off_t offset,
                      struct fuse_file_info* fi) {
    if (!files.count(path))
        return -ENOENT;

    const auto& content = files[path].content;
    size_t len = content.size();
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, content.c_str() + offset, size);
    } else {
        size = 0;
    }
    return size;
}

// Write to file
static int memfs_write(const char* path, const char* buf, size_t size,
                       off_t offset, struct fuse_file_info* fi) {
    if (!files.count(path))
        return -ENOENT;

    auto& content = files[path].content;
    if (offset + size > content.size())
        content.resize(offset + size);
    memcpy(&content[offset], buf, size);
    return size;
}

// Create file
static int memfs_create(const char* path, mode_t mode,
                        struct fuse_file_info* fi) {
    if (files.count(path))
        return -EEXIST;

    files[path] = {path, "", mode};
    return 0;
}

// Remove file
static int memfs_unlink(const char* path) {
    if (!files.count(path))
        return -ENOENT;

    files.erase(path);
    return 0;
}

static struct fuse_operations memfs_oper = {
    .getattr = memfs_getattr,
    .readdir = memfs_readdir,
    .read = memfs_read,
    .write = memfs_write,
    .create = memfs_create,
    .unlink = memfs_unlink,
};

int main(int argc, char* argv[]) {
    return fuse_main(argc, argv, &memfs_oper, NULL);
}
