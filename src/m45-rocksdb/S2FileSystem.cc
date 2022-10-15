/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include "S2FileSystem.h"
#include <string>
#include <iostream>
#include <sys/mman.h>
#include <stosys_debug.h>
#include <utils.h>

namespace ROCKSDB_NAMESPACE
{

    

    
    /*
    void MYFS_DeletePath(MYFS *FSObj, std::string path)
    {
        MYFS::Inode *ptr;
        int isPresent = Get_Path_Inode(FSObj, path, &ptr);
        if (isPresent)
            return;
        // TODO: Handle logic if dir
        // Free data block of MYFS::Inode as well!

        // Update Parent
        std::string ppath;
        Get_ParentPath(path, ppath);
        // Delete from lookup map
    }
    */
   

    S2FileSystem::S2FileSystem(std::string uri_db_path, bool debug)
    {
        FileSystem::Default();
        std::string sdelimiter = ":";
        std::string edelimiter = "://";
        this->_uri = uri_db_path;
        struct zdev_init_params params;
        std::string device = uri_db_path.substr(uri_db_path.find(sdelimiter) + sdelimiter.size(),
                                                uri_db_path.find(edelimiter) -
                                                    (uri_db_path.find(sdelimiter) + sdelimiter.size()));
        // make sure to setup these parameters properly and check the forced reset flag for M5
        params.name = strdup(device.c_str());
        params.log_zones = 3;
        params.gc_wmark = 1;
        params.force_reset = true;
        int ret = init_ss_zns_device(&params, &this->_zns_dev);
        free(params.name);
        if (ret != 0)
        {
            std::cout << "Error: " << uri_db_path << " failed to open the device " << device.c_str() << "\n";
            std::cout << "Error: ret " << ret << "\n";
        }
        assert(ret == 0);
        assert(this->_zns_dev->lba_size_bytes != 0);
        assert(this->_zns_dev->capacity_bytes != 0);
        ss_dprintf(DBG_FS_1, "device %s is opened and initialized, reported LBA size is %u and capacity %lu \n",
                   device.c_str(), this->_zns_dev->lba_size_bytes, this->_zns_dev->capacity_bytes);

        // INIT File System
        // TODO: In case of persistency; Read following data from Super block
        // Init Bitmaps from disk
        if (debug)
            std::cout << "Init MYFS" << std::endl;
        this->FileSystemObj = (MYFS::MYFS_META *)calloc(1, sizeof(MYFS::MYFS_META));
        this->FileSystemObj->zns = this->_zns_dev;
        this->FileSystemObj->FileSystemCapacity = this->_zns_dev->capacity_bytes;
        this->FileSystemObj->LogicalBlockSize = this->_zns_dev->lba_size_bytes;
        // We reserve a single block as super block and MAX_INODE_COUNT as
        this->FileSystemObj->DataBlockCount = (this->FileSystemObj->FileSystemCapacity / this->FileSystemObj->LogicalBlockSize - (MAX_INODE_COUNT + 1));
        if (debug)
            std::cout << "File System params : " << this->FileSystemObj->FileSystemCapacity << " " << this->FileSystemObj->LogicalBlockSize << " " << this->FileSystemObj->DataBlockCount << std::endl;

        // Init Data blocks bitmap
        // this->FileSystemObj->LookupCache = (mapEntries *) calloc(LOOKUP_MAP_SIZE, sizeof(mapEntries));
        this->FileSystemObj->DataBitMap = (bool *)calloc(this->FileSystemObj->DataBlockCount, sizeof(bool));

        // Init root inode
        // TODO: In case of persistency check if already present in disk
        // FIXME: Get root dir name dynamically
        this->FileSystemObj->DataBlockPtr = 0; // Reserved for Root Node
        this->FileSystemObj->InodePtr = 0;
        this->FileSystemObj->InodeBitMap[0] = true;
        *(this->FileSystemObj->DataBitMap) = true;
        this->FileSystemObj->rootEntry = (MYFS::Inode *)calloc(1, sizeof(MYFS::Inode));
        strcpy(this->FileSystemObj->rootEntry->EntityName, "tmp");
        this->FileSystemObj->rootEntry->IsDir = true;
        this->FileSystemObj->rootEntry->Inode_no = 0;
        this->FileSystemObj->rootEntry->FileSize = 0;
        this->FileSystemObj->rootEntry->Direct_data_lbas[0] = DATA_BLOCKS_OFFSET * this->FileSystemObj->LogicalBlockSize;
    }

    S2FileSystem::~S2FileSystem()
    {
        deinit_ss_zns_device(this->FileSystemObj->zns);
        //TODO: Store before Free
        free(this->FileSystemObj->rootEntry);
        free(this->FileSystemObj->DataBitMap);

        for(int i=0;i<LOOKUP_MAP_SIZE;i++) {
            MYFS::mapEntries *head = this->FileSystemObj->LookupCache[i], *tmp;
            while(head!=NULL) {
                tmp = head;
                head = head->chain;
                free(tmp->ptr);
                free(tmp);
            }
        }
        free(this->FileSystemObj);
    }

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores nullptr in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    IOStatus S2FileSystem::NewSequentialFile(const std::string &fname, const FileOptions &file_opts,
                                             std::unique_ptr<FSSequentialFile> *result, IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Clean_Path(fname, cpath);
        MYFS::Inode *ptr;
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);

        result->reset();
        result->reset(new MYFS::MYFS_SequentialFile(cpath, this->FileSystemObj));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::IsDirectory(const std::string &, const IOOptions &options, bool *is_dir, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    IOStatus S2FileSystem::NewRandomAccessFile(const std::string &fname, const FileOptions &file_opts,
                                               std::unique_ptr<FSRandomAccessFile> *result, IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Clean_Path(fname, cpath);
        MYFS::Inode *ptr;
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);

        result->reset();
        result->reset(new MYFS::MYFS_RandomAccessFile(cpath, this->FileSystemObj));
        return IOStatus::OK();
    }

    const char *S2FileSystem::Name() const
    {
        return "S2FileSytem";
    }

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    IOStatus S2FileSystem::NewWritableFile(const std::string &fname, const FileOptions &file_opts,
                                           std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Clean_Path(fname, cpath);
        MYFS::Inode *ptr;
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            MYFS::MYFS_CreateFile(this->FileSystemObj, cpath);
        else
            ptr->FileSize = 0;

        result->reset();
        result->reset(new MYFS::MYFS_WritableFile(cpath, this->FileSystemObj));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::ReopenWritableFile(const std::string &, const FileOptions &, std::unique_ptr<FSWritableFile> *,
                                              IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewRandomRWFile(const std::string &, const FileOptions &, std::unique_ptr<FSRandomRWFile> *,
                                           IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NewMemoryMappedFileBuffer(const std::string &, std::unique_ptr<MemoryMappedFileBuffer> *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create an object that represents a directory. Will fail if directory
    // doesn't exist. If the directory exists, it will open the directory
    // and create a new Directory object.
    //
    // On success, stores a pointer to the new Directory in
    // *result and returns OK. On failure stores nullptr in *result and
    // returns non-OK.
    IOStatus
    S2FileSystem::NewDirectory(const std::string &name, const IOOptions &io_opts, std::unique_ptr<FSDirectory> *result,
                               IODebugContext *dbg)
    {
        result->reset();
        result->reset(new MYFS::MYFS_Directory(this->FileSystemObj));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::GetFreeSpace(const std::string &, const IOOptions &, uint64_t *, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::Truncate(const std::string &, size_t, const IOOptions &, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Create the specified directory. Returns error if directory exists.
    IOStatus S2FileSystem::CreateDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Clean_Path(dirname, cpath);
        MYFS::Inode *ptr;
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            isPresent = MYFS::MYFS_CreateDir(this->FileSystemObj, cpath);
        else
            return IOStatus::IOError(__FUNCTION__);

        return IOStatus::OK();
    }

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    IOStatus S2FileSystem::CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Clean_Path(dirname, cpath);
        MYFS::Inode *ptr;
        std::string dir = cpath.substr(0, cpath.size() - 1);
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, dir, &ptr);
        if (isPresent)
            isPresent = MYFS::MYFS_CreateDir(this->FileSystemObj, dir);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }

    IOStatus
    S2FileSystem::GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg)
    {
        
        std::string cpath;
        MYFS::Clean_Path(fname, cpath);
        MYFS::Inode *ptr;
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);
        else
            *file_size = ptr->FileSize;
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::DeleteDir(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetFileModificationTime(const std::string &fname, const IOOptions &options, uint64_t *file_mtime,
                                                   IODebugContext *dbg)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetAbsolutePath(const std::string &db_path, const IOOptions &options, std::string *output_path,
                                           IODebugContext *dbg)
    {
        //*output_path = db_path;
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::DeleteFile(const std::string &fname, const IOOptions &options, IODebugContext *dbg)
    {
        // MYFS_DeletePath(this->FileSystemObj, fname);
        std::cout<<"Delete file called"<<std::endl;
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::NewLogger(const std::string &fname, const IOOptions &io_opts, std::shared_ptr<Logger> *result,
                                     IODebugContext *dbg)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::GetTestDirectory(const IOOptions &options, std::string *path, IODebugContext *dbg)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // Release the lock acquired by a previous successful call to LockFile.
    // REQUIRES: lock was returned by a successful LockFile() call
    // REQUIRES: lock has not already been unlocked.
    IOStatus S2FileSystem::UnlockFile(FileLock *lock, const IOOptions &options, IODebugContext *dbg)
    {
        return IOStatus::OK();
    }

    // Lock the specified file.  Used to prevent concurrent access to
    // the same db by multiple processes.  On failure, stores nullptr in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK.  The caller should call
    // UnlockFile(*lock) to release the lock.  If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure.  I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    IOStatus S2FileSystem::LockFile(const std::string &fname, const IOOptions &options, FileLock **lock, IODebugContext *dbg)
    {
        return IOStatus::OK();
    }

    IOStatus
    S2FileSystem::AreFilesSame(const std::string &, const std::string &, const IOOptions &, bool *, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::NumFileLinks(const std::string &, const IOOptions &, uint64_t *, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::LinkFile(const std::string &, const std::string &, const IOOptions &, IODebugContext *)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    IOStatus S2FileSystem::RenameFile(const std::string &src, const std::string &target, const IOOptions &options,
                                      IODebugContext *dbg)
    {
        std::string cpath_target, cpath_src;
        MYFS::Clean_Path(src, cpath_src);
        MYFS::Clean_Path(target, cpath_target);
        MYFS::Inode *targetptr, *sourceptr;
        // MYFS_DeletePath(this->FileSystemObj, target);
        // FIXME: Logic for rename
        // Change name in Inode
        // Change in parent

        // verify if target exists
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath_target, &targetptr);
        if (isPresent)
        {
            // if it is not present
            // rename the inode
            std::string entityName;
            MYFS::Get_EntityName(cpath_target, entityName);
            MYFS::Get_Path_Inode(this->FileSystemObj, cpath_src, &sourceptr);
            strcpy(sourceptr->EntityName, entityName.c_str());
            MYFS::LookupMap_Delete(this->FileSystemObj, cpath_src);

            MYFS::LookupMap_Insert(this->FileSystemObj, cpath_target, sourceptr);
            // rename the entity in the parent
            std::string srcEntityName;
            MYFS::Get_EntityName(cpath_src, srcEntityName);

            std::string parentPath;
            MYFS::Get_ParentPath(cpath_target, parentPath);
            int parentUpdated = MYFS::Rename_Child_In_Parent(this->FileSystemObj, parentPath, entityName, srcEntityName);
            if (parentUpdated)
                return IOStatus::IOError(__FUNCTION__);
        }
        else
        {
        }
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::GetChildrenFileAttributes(const std::string &dir, const IOOptions &options,
                                                     std::vector<FileAttributes> *result, IODebugContext *dbg)
    {
        return FileSystem::GetChildrenFileAttributes(dir, options, result, dbg);
    }

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    IOStatus S2FileSystem::GetChildren(const std::string &dir, const IOOptions &options, std::vector<std::string> *result,
                                       IODebugContext *dbg)
    {
        std::string cpath;
        MYFS::Get_ParentPath(dir, cpath);
        MYFS::Inode *ptr;
        
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);
        uint32_t err = MYFS::Load_Children(this->FileSystemObj, ptr, "", result, true, "");
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }

    // Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    IOStatus S2FileSystem::FileExists(const std::string &fname, const IOOptions &options, IODebugContext *dbg)
    {
        MYFS::Inode *ptr;
        std::string cpath;
        MYFS::Clean_Path(fname, cpath);
        int isPresent = MYFS::Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::NotFound();
        return IOStatus::OK();
    }

    IOStatus
    S2FileSystem::ReuseWritableFile(const std::string &fname, const std::string &old_fname, const FileOptions &file_opts,
                                    std::unique_ptr<FSWritableFile> *result, IODebugContext *dbg)
    {
        return IOStatus::IOError(__FUNCTION__);
    }

    // MYFS_Directory::MYFS_Directory(std::string name) {
    //     std::cout<<"For checl"<<std::endl;
    // }
    /*
    IOStatus MYFS_WritableFile::PositionedAppend(const Slice &data, uint64_t offset, const IOOptions &opts,
                                                 IODebugContext *dbg)
    {

        char *block = (char *)data.data();
        uint64_t size = data.size();
        int err = this->fp->PAppend(offset, size, block);
        std::cout<<"PAppend size : "<<size<<" "<<offset<<std::endl;
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }
    */
}
