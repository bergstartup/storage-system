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
    int LookupMap_HashFunction(std::string id)
    {
        unsigned hashindex;
        char *ptr = const_cast<char *>(id.c_str());
        for (hashindex = 0; *ptr != '\0'; ptr++)
            hashindex = *ptr + STRINGENCODE * hashindex;
        return hashindex % LOOKUP_MAP_SIZE;
    }

    int LookupMap_Insert(MYFS *FSObj, std::string id, Inode *ptr)
    {
        int index = LookupMap_HashFunction(id);

        mapEntries *map = (mapEntries *)calloc(1, sizeof(mapEntries));
        strcpy(map->id,id.c_str());
        map->ptr = ptr;
        map->chain = NULL;

        if (FSObj->LookupCache[index] == NULL)
            FSObj->LookupCache[index] = map;
        else
        {
            struct mapEntries *head;
            head = FSObj->LookupCache[index];
            while (head->chain != NULL)
                head = head->chain;
            head->chain = map;
        }

        return 0;
    }

    int LookupMap_Delete(MYFS *FSObj, std::string id)
    {
        int index = LookupMap_HashFunction(id);
        struct mapEntries *head, *tmp = NULL;
        head = FSObj->LookupCache[index];

        while (head != NULL)
        {
            if (!strcmp(head->id,id.c_str()))
            {
                if (tmp == NULL)
                    FSObj->LookupCache[index] = head->chain;
                else
                    tmp->chain = head->chain;
                free(head);
                break;
            }
            tmp = head;
            head = head->chain;
        }

        return 0;
    }

    int LookupMap_Lookup(MYFS *FSObj, std::string id, Inode **ptr)
    {
        int index = LookupMap_HashFunction(id);
        struct mapEntries *head;
        head = FSObj->LookupCache[index];

        while (head != NULL)
        {
            if (!strcmp(head->id,id.c_str()))
                break;
            head = head->chain;
        }

        if (head == NULL)
            return -1;

        *ptr = head->ptr;
        return 0;
    }

    int Load_From_NVM(MYFS *FSObj, uint64_t addr, void *buffer, uint64_t size)
    {
        // Check the size if quantization of LBA
        int err = zns_udevice_read(FSObj->zns, addr, buffer, size);
        std::cout<<"Load from NVM : "<<err<<std::endl;
        return 0;
    }

    int Store_To_NVM(MYFS *FSObj, uint64_t addr, void *buffer, uint64_t size)
    {
        int err = zns_udevice_write(FSObj->zns, addr, buffer, size);
        return 0;
    }

    uint32_t get_FreeInode(MYFS *FSObj)
    {
        uint32_t ptr = (FSObj->InodePtr + 1) % MAX_INODE_COUNT;
        while (ptr != FSObj->InodePtr)
        {
            if (!FSObj->InodeBitMap[ptr])
            {
                FSObj->InodePtr = ptr;
                FSObj->InodeBitMap[ptr] = true;
                return ptr;
            }
            ptr = (ptr + 1) % MAX_INODE_COUNT;
        }
        return 0;
    }

    uint64_t get_FreeDataBlock(MYFS *FSObj)
    {
        uint64_t ptr = (FSObj->DataBlockPtr + 1) % FSObj->DataBlockCount;
        while (ptr != FSObj->DataBlockPtr)
        {
            if (!FSObj->DataBitMap[ptr])
            {
                FSObj->DataBlockPtr = ptr;
                FSObj->DataBitMap[ptr] = true;
                return (ptr + DATA_BLOCKS_OFFSET) * FSObj->LogicalBlockSize;
            }
            ptr = (ptr + 1) % FSObj->DataBlockCount;
        }
        return 0;
    }

    /*
    void free_DataBlock(MYFS *FSObj, uint64_t addr)
    {
        int index = (addr / FSObj->LogicalBlockSize) - DATA_BLOCKS_OFFSET;
        FSObj->DataBitMap[index] = false;
    }
    */

    // Trim till /../path in /../path/name
    void Get_ParentPath(std::string path, std::string &parent)
    {
        int index;
        for (int i = path.size() - 1; i >= 0; i--)
        {
            if (path[i] == '/')
            {
                index = i;
                break;
            }
        }
        // Trim if additional slash is present
        if (path[index - 1] == '/')
            index--;

        parent = path.substr(0, index);
    }

    // Trim /../path/name to name
    void Get_EntityName(std::string path, std::string &entityName)
    {
        int index;
        for (int i = path.size() - 1; i >= 0; i--)
        {
            if (path[i] == '/')
            {
                index = i;
                break;
            }
        }
        entityName = path.substr(index + 1, path.size());
    }

    void Clean_Path(std::string path, std::string &newPath)
    {
        std::string entity;
        Get_EntityName(path, entity);
        Get_ParentPath(path, newPath);
        newPath.append("/");
        newPath.append(entity);
    }

    // Load_Childrent function reads DIR's data, either store children names in vector or return inode of asked child depending on bool
    // return value will be 0 if asked child is not present
    uint32_t Load_Children(MYFS *FSObj, Inode *ptr, std::string entityName, std::vector<std::string> *children, bool loadChildren, std::string targetName = "")
    {
        
        // Check no of children and load it
        uint64_t children_count = ptr->FileSize;

        MYFS_Dir *dir_ptr = (MYFS_Dir *)calloc(1, sizeof(MYFS_Dir));
        for (int i = 0; i < children_count / 16; i++)
        {
            Load_From_NVM(FSObj, ptr->Direct_data_lbas[i], dir_ptr, 4096);
            for (int j = 0; j < 16; j++)
            {
                if (loadChildren) {
                    if(strcmp(dir_ptr->Entities[i].EntityName,"<del>"))
                        children->push_back(dir_ptr->Entities[i].EntityName);
                }
                else
                {
                    if (!strcmp(dir_ptr->Entities[j].EntityName, entityName.c_str()))
                    {
                        if (targetName == "")
                        {
                            uint32_t ret = dir_ptr->Entities[j].InodeNum;
                            free(dir_ptr);
                            return ret;
                        }
                        else
                        {
                            strcpy(dir_ptr->Entities[j].EntityName, targetName.c_str());
                            Store_To_NVM(FSObj, ptr->Direct_data_lbas[i], dir_ptr, 4096);
                            free(dir_ptr);
                            return 0;
                        }
                    }
                }
            }
        }

        Load_From_NVM(FSObj, ptr->Direct_data_lbas[children_count / 16], dir_ptr, 4096);
        for (int i = 0; i < children_count % 16; i++)
        {
            if (loadChildren) {
                if(strcmp(dir_ptr->Entities[i].EntityName,"<del>"))
                    children->push_back(dir_ptr->Entities[i].EntityName);
            }
            else
            {
                if (!strcmp(dir_ptr->Entities[i].EntityName, entityName.c_str()))
                {
                    if (targetName == "")
                    {
                        uint32_t ret = dir_ptr->Entities[i].InodeNum;
                        free(dir_ptr);
                        return ret;
                    }
                    else
                    {
                        strcpy(dir_ptr->Entities[i].EntityName, targetName.c_str());
                        Store_To_NVM(FSObj, ptr->Direct_data_lbas[children_count / 16], dir_ptr, 4096);
                        free(dir_ptr);
                        return 0;
                    }
                }
            }
        }
        free(dir_ptr);
        return 0;
    }

    // A recursive call to load inode of the given path to lookupmap
    // Stores the inode ptr as well, returns 0 in success
    int Get_Path_Inode(MYFS *FSObj, std::string path, Inode **ptr)
    {

        if (path == "/tmp")
        {
            *ptr = FSObj->rootEntry;
            return 0;
        }

        // Check if path in lookupMap cache
        int isPresent = LookupMap_Lookup(FSObj, path, ptr);
        if (!isPresent)
            return 0;

        // if not : Get_Path_Inode for parent dir
        std::string parent;
        Inode *parentInode;
        Get_ParentPath(path, parent);
        isPresent = Get_Path_Inode(FSObj, parent, &parentInode);
        if (isPresent)
            return -1;
        // Read parent dir and get asked inode number
        if (parentInode->FileSize == 0)
            return -1;
        // Get Entity to search for
        std::string entityName;
        Get_EntityName(path, entityName);
        uint32_t index = Load_Children(FSObj, parentInode, entityName, NULL, false);
        if (!index)
            return -1;

        // Load the children index inode from disk and store in lookupMap;
        uint64_t address = SUPER_BLOCK_SIZE + index * INODE_SIZE;
        Inode *iptr = (Inode *)calloc(1, sizeof(Inode));

        Load_From_NVM(FSObj, address, iptr, INODE_SIZE);
        std::cout<<"Load File : "<<entityName<<" "<<index<<" @"<<address<<" But,"<<iptr->EntityName<<" "<<iptr->Inode_no<<std::endl;
        //if (notPresent)
        //   return -1;

        // Put it in lookup Map
        LookupMap_Insert(FSObj, path, iptr);
        *ptr = iptr;
        return 0;
    }

    //Renaming
    int Rename_Child_In_Parent(MYFS *FSObj, std::string Ppath, std::string srcName, std::string targetName)
    {
        // FIXME: Logic for rename
        Inode *parentInode;
        int isPresent = Get_Path_Inode(FSObj, Ppath, &parentInode);
        uint32_t rename = Load_Children(FSObj, parentInode, srcName, NULL, false, targetName);
        return rename;
    }

     void MYFS_DeletePath(MYFS *FSObj, std::string path)
    {
        Inode *ptr;
        int notPresent = Get_Path_Inode(FSObj, path, &ptr);
        if (notPresent)
            return;
        
        //Update parent
        std::string entityName, ppath;
        Get_EntityName(path, entityName);
        Get_ParentPath(path, ppath);
        Rename_Child_In_Parent(FSObj, ppath, entityName, "<del>");
        
        //Change lookupmap
        LookupMap_Delete(FSObj, path);
        FSObj->InodeBitMap[ptr->Inode_no] = false;
        //Free Data zones
        free(ptr);
    }

    //For creation
    int Update_Parent(MYFS *FSObj, std::string Ppath, std::string childName, uint32_t childInode, bool del = false)
    {
        // FIXME: Logic for deletion
        Inode *ptr;
        int isPresent = Get_Path_Inode(FSObj, Ppath, &ptr);
        if (isPresent)
            return -1;

        MYFS_DirData dirDataptr;
        strcpy(dirDataptr.EntityName, childName.c_str());
        dirDataptr.InodeNum = childInode;

        MYFS_Dir *dirPtr;
        dirPtr = (MYFS_Dir *)calloc(1, sizeof(MYFS_Dir));
        int index = (++ptr->FileSize) / 16;
        uint64_t addr = ptr->Direct_data_lbas[index];

        if (!addr)
        {
            addr = get_FreeDataBlock(FSObj);
            ptr->Direct_data_lbas[index] = addr;
        }
        else
        {
            index = Load_From_NVM(FSObj, addr, dirPtr, 4096);
            if (index)
                return -1;
        }

        index = ptr->FileSize % 16;
        dirPtr->Entities[index - 1] = dirDataptr;
        Store_To_NVM(FSObj, addr, dirPtr, 4096);
        free(dirPtr);

        return 0;
    }

   

    int MYFS_CreateFile(MYFS *FSObj, std::string path)
    {
        uint32_t inode_no = get_FreeInode(FSObj);
        Inode *ptr = (Inode *)calloc(1, sizeof(Inode));
        // Fill the ptr
        std::string entityName;
        Get_EntityName(path, entityName);
        strcpy(ptr->EntityName, entityName.c_str());
        ptr->Inode_no = inode_no;

        // Update parent
        std::string parent;
        Get_ParentPath(path, parent);
        int parentUpdated = Update_Parent(FSObj, parent, entityName, inode_no);
        if (parentUpdated)
            return -1;

        // Load to lookupmap
        LookupMap_Insert(FSObj, path, ptr);

        return 0;
    }

    int MYFS_CreateDir(MYFS *FSObj, std::string path)
    {
        uint32_t inode_no = get_FreeInode(FSObj);
        Inode *ptr = (Inode *)calloc(1, sizeof(Inode));
        // Fill the ptr
        std::string entityName;
        Get_EntityName(path, entityName);
        strcpy(ptr->EntityName, entityName.c_str());
        ptr->IsDir = true;
        ptr->Inode_no = inode_no;

        // Update parent
        std::string parent;
        Get_ParentPath(path, parent);
        int parentUpdated = Update_Parent(FSObj, parent, entityName, inode_no);
        if (parentUpdated)
            return -1;

        // Load to lookupmap
        LookupMap_Insert(FSObj, path, ptr);

        return 0;
    }

    int initFS(MYFS *FSObj, user_zns_device *zns)
    {
        FSObj->zns = zns;
        FSObj->FileSystemCapacity = zns->capacity_bytes;
        FSObj->LogicalBlockSize = zns->lba_size_bytes;
        // We reserve a single block as super block and MAX_INODE_COUNT as
        FSObj->DataBlockCount = (FSObj->FileSystemCapacity / FSObj->LogicalBlockSize - (MAX_INODE_COUNT + 1));

        FSObj->rootEntry = (Inode *)calloc(1, sizeof(Inode));
        FSObj->DataBitMap = (bool *)calloc(FSObj->DataBlockCount, sizeof(bool));
        
        // this->FileSystemObj->LookupCache = (mapEntries *) calloc(LOOKUP_MAP_SIZE, sizeof(mapEntries));
        void *ptr = (void *) calloc(1, SUPER_BLOCK_SIZE);
        Load_From_NVM(FSObj, 0, ptr, SUPER_BLOCK_SIZE);
        struct SuperBlock *sb = (SuperBlock *) ptr;
        //memcpy(sb, ptr, sizeof(SuperBlock));

        if(!sb->persistent) {
            //Not stored in disk
            FSObj->DataBlockPtr = 0; // Reserved for Root Node
            FSObj->InodePtr = 0;
            FSObj->InodeBitMap[0] = true;
            *(FSObj->DataBitMap) = true;
            
            //Do the following only if already not present
            strcpy(FSObj->rootEntry->EntityName, "tmp");
            FSObj->rootEntry->IsDir = true;
            FSObj->rootEntry->Inode_no = 0;
            FSObj->rootEntry->FileSize = 0;
            FSObj->rootEntry->Direct_data_lbas[0] = DATA_BLOCKS_OFFSET * FSObj->LogicalBlockSize;
        } else {
            //Load root inode; Stored in disk
            Load_From_NVM(FSObj, SUPER_BLOCK_SIZE, FSObj->rootEntry, INODE_SIZE);
            FSObj->DataBlockPtr = sb->dataBlockPtr;
            FSObj->InodePtr = sb->inodeBlockPtr;
            memcpy(FSObj->InodeBitMap, ptr+sizeof(SuperBlock), sizeof(FSObj->InodeBitMap));
            memcpy(FSObj->DataBitMap, ptr+sizeof(SuperBlock)+sizeof(FSObj->InodeBitMap), FSObj->DataBlockCount);
        }
        free(ptr);
        //free(sb);
        return 0;
    }

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
        //make sure to setup these parameters properly and check the forced reset flag for M5
        params.name = strdup(device.c_str());
        params.log_zones = 3;
        params.gc_wmark = 1;
        params.force_reset = false;
        int ret = init_ss_zns_device(&params, &this->_zns_dev);
        free(params.name);
        if(ret != 0){
            std::cout << "Error: " << uri_db_path << " failed to open the device " << device.c_str() << "\n";
            std::cout << "Error: ret " << ret << "\n";
        }
        assert (ret == 0);
        assert(this->_zns_dev->lba_size_bytes != 0);
        assert(this->_zns_dev->capacity_bytes != 0);
        ss_dprintf(DBG_FS_1, "device %s is opened and initialized, reported LBA size is %u and capacity %lu \n",
                   device.c_str(), this->_zns_dev->lba_size_bytes, this->_zns_dev->capacity_bytes);

        // INIT File System
        this->FileSystemObj = (MYFS *)calloc(1, sizeof(MYFS));
        initFS(this->FileSystemObj, this->_zns_dev);        
    }

    S2FileSystem::~S2FileSystem()
    {
        Store_To_NVM(this->FileSystemObj, SUPER_BLOCK_SIZE, this->FileSystemObj->rootEntry, INODE_SIZE);
        free(this->FileSystemObj->rootEntry);

        //Store all inodes from lookup cache to disk
        for(int i=0;i<LOOKUP_MAP_SIZE;i++) {
            mapEntries *head = this->FileSystemObj->LookupCache[i], *tmp;
            while(head!=NULL) {
                tmp = head;
                head = head->chain;
                Store_To_NVM(this->FileSystemObj, (tmp->ptr->Inode_no * INODE_SIZE) + SUPER_BLOCK_SIZE, tmp->ptr, INODE_SIZE);
                std::cout<<"File : "<<tmp->ptr->EntityName<<" "<<tmp->ptr->Inode_no<<" @ "<<(tmp->ptr->Inode_no * INODE_SIZE) + SUPER_BLOCK_SIZE<<std::endl;
                free(tmp->ptr);
                free(tmp);
            }
        }

        void *superBlockWBitMap = (void *) calloc(1,SUPER_BLOCK_SIZE);
        struct SuperBlock *sb = (SuperBlock *) superBlockWBitMap;//calloc(1, sizeof(SuperBlock));
        sb->dataBlockPtr = this->FileSystemObj->DataBlockPtr;
        sb->inodeBlockPtr = this->FileSystemObj->InodePtr;
        sb->persistent = true;
        std::cout<<"Inode count : "<<MAX_INODE_COUNT<<" "<<this->FileSystemObj->DataBlockCount<<std::endl;
        //memcpy(superBlockWBitMap, sb, sizeof(SuperBlock));
        memcpy(superBlockWBitMap+sizeof(SuperBlock), this->FileSystemObj->InodeBitMap, MAX_INODE_COUNT);
        memcpy(superBlockWBitMap+sizeof(SuperBlock)+MAX_INODE_COUNT, this->FileSystemObj->DataBitMap, this->FileSystemObj->DataBlockCount);
        Store_To_NVM(this->FileSystemObj, 0, superBlockWBitMap, SUPER_BLOCK_SIZE);
        free(superBlockWBitMap);
        //free(sb);
        free(this->FileSystemObj->DataBitMap);
        deinit_ss_zns_device(this->FileSystemObj->zns);
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
        Clean_Path(fname, cpath);
        Inode *ptr;
        int notPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (notPresent)
            return IOStatus::IOError(__FUNCTION__);

        result->reset();
        result->reset(new MYFS_SequentialFile(cpath, this->FileSystemObj));
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
        Clean_Path(fname, cpath);
        Inode *ptr;
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);

        result->reset();
        result->reset(new MYFS_RandomAccessFile(cpath, this->FileSystemObj));
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
        Clean_Path(fname, cpath);
        Inode *ptr;
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            MYFS_CreateFile(this->FileSystemObj, cpath);
        else
            ptr->FileSize = 0;

        result->reset();
        result->reset(new MYFS_WritableFile(cpath, this->FileSystemObj));
        return IOStatus::OK();
    }

    IOStatus S2FileSystem::ReopenWritableFile(const std::string &fname, const FileOptions &, std::unique_ptr<FSWritableFile> *result,
                                              IODebugContext *)
    {
        std::string cpath;
        Clean_Path(fname, cpath);
        Inode *ptr;
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError();

        result->reset();
        result->reset(new MYFS_WritableFile(cpath, this->FileSystemObj));
        return IOStatus::OK();
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
        result->reset(new MYFS_Directory(this->FileSystemObj));
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
        Clean_Path(dirname, cpath);
        Inode *ptr;
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            isPresent = MYFS_CreateDir(this->FileSystemObj, cpath);
        else
            return IOStatus::IOError(__FUNCTION__);

        return IOStatus::OK();
    }

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    IOStatus S2FileSystem::CreateDirIfMissing(const std::string &dirname, const IOOptions &options, IODebugContext *dbg)
    {
        std::string cpath;
        Clean_Path(dirname, cpath);
        Inode *ptr;
        std::string dir = cpath.substr(0, cpath.size() - 1);
        int isPresent = Get_Path_Inode(this->FileSystemObj, dir, &ptr);
        if (isPresent)
            isPresent = MYFS_CreateDir(this->FileSystemObj, dir);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }

    IOStatus
    S2FileSystem::GetFileSize(const std::string &fname, const IOOptions &options, uint64_t *file_size, IODebugContext *dbg)
    {
        
        std::string cpath;
        Clean_Path(fname, cpath);
        Inode *ptr;
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
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
        Clean_Path(src, cpath_src);
        Clean_Path(target, cpath_target);
        Inode *targetptr, *sourceptr;


        // verify if target exists
        int notPresent = Get_Path_Inode(this->FileSystemObj, cpath_target, &targetptr);
        if (!notPresent)
            //If present
            MYFS_DeletePath(this->FileSystemObj, cpath_target);
        
        // if it is not present
        // rename the inode
        std::string entityName;
        Get_EntityName(cpath_src, entityName);
        Get_Path_Inode(this->FileSystemObj, cpath_src, &sourceptr);
        LookupMap_Delete(this->FileSystemObj, cpath_src);

        LookupMap_Insert(this->FileSystemObj, cpath_target, sourceptr);
        std::string targetEntityName;
        Get_EntityName(cpath_target, targetEntityName);
        strcpy(sourceptr->EntityName, targetEntityName.c_str());

        std::string parentPath;
        Get_ParentPath(cpath_target, parentPath);
        int parentUpdated = Rename_Child_In_Parent(this->FileSystemObj, parentPath, entityName, targetEntityName);
        if (parentUpdated)
            return IOStatus::IOError(__FUNCTION__);
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
        Get_ParentPath(dir, cpath);
        Inode *ptr;
        
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
        if (isPresent)
            return IOStatus::IOError(__FUNCTION__);
        uint32_t err = Load_Children(this->FileSystemObj, ptr, "", result, true);
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
        Inode *ptr;
        std::string cpath;
        Clean_Path(fname, cpath);
        int isPresent = Get_Path_Inode(this->FileSystemObj, cpath, &ptr);
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

    int load_nth_indirect_block(MYFS *FSObj, uint32_t n, uint64_t indirect_lba, Indirect_ptr **ptr)
    {
        for (int i = 0; i < n; i++)
            Load_From_NVM(FSObj, (*ptr)->Indirect_ptr_lbas, *ptr, 4096);
    }

    int get_blocks_addr(MYFS *FSObj, Inode *ptr, uint64_t offset, uint64_t size, std::vector<uint64_t> *addresses, bool forWrite)
    {
        uint32_t curr = offset / 4096, end = (offset+size) / 4096;
        uint64_t if_dirty_addr;
        uint64_t *data_block_lba_ptr, next_indirect_block_addr;
        uint32_t no_of_data_block_ptrs;
        Indirect_ptr *iptr = NULL;
        // Load the direct ptr
        if (curr < 480)
        {
            // In Inode block itself
            data_block_lba_ptr = ptr->Direct_data_lbas;
            no_of_data_block_ptrs = 480;
            next_indirect_block_addr = ptr->Indirect_ptr_lbas;
            if_dirty_addr = 4096 + (ptr->Inode_no * INODE_SIZE);
        }
        else
        {
            curr -= 480;
            int nth_indirect = curr / 510;
            //What if ptr->Indirect_ptr_lba 
            iptr = (Indirect_ptr *)calloc(1, 4096);
            if(ptr->Indirect_ptr_lbas == 0) {
                ptr->Indirect_ptr_lbas = get_FreeDataBlock(FSObj);
            }

            Load_From_NVM(FSObj, ptr->Indirect_ptr_lbas, iptr, 4096);
            for (int i = 0; i < nth_indirect; i++)
                Load_From_NVM(FSObj, iptr->Indirect_ptr_lbas, iptr, 4096);

            data_block_lba_ptr = iptr->Direct_data_lbas;
            next_indirect_block_addr = iptr->Indirect_ptr_lbas;
            no_of_data_block_ptrs = 510;
            curr = curr % 510;
            if_dirty_addr = iptr->Current_addr;
        }

        uint64_t addr;
        for (int i = 0; i <= end; i++)
        {
            addr = *(data_block_lba_ptr + curr);
            if (!addr)
            {
                addr = get_FreeDataBlock(FSObj);
                *(data_block_lba_ptr+curr) = addr;
            }
            addresses->push_back(addr);
            curr++;

            if (curr == no_of_data_block_ptrs)
            {
                if (!next_indirect_block_addr)
                {
                    // If no indirect block ptr, create one and store to mem
                    next_indirect_block_addr = get_FreeDataBlock(FSObj);
                    if (iptr == NULL)
                    {
                        ptr->Indirect_ptr_lbas = next_indirect_block_addr;
                        Store_To_NVM(FSObj, 4096 + (ptr->Inode_no * INODE_SIZE), ptr, 4096);
                        
                    }
                    else
                    {
                        iptr->Indirect_ptr_lbas = next_indirect_block_addr;
                        Store_To_NVM(FSObj, iptr->Current_addr, iptr, 4096);
                        free(iptr);
                    }
                    iptr = (Indirect_ptr *)calloc(1, 4096);
                    iptr->Current_addr = next_indirect_block_addr;
                }
                else
                {
                    if (iptr == NULL)
                        iptr = (Indirect_ptr *)calloc(1, 4096);

                    Load_From_NVM(FSObj, next_indirect_block_addr, iptr, 4096);
                }
                next_indirect_block_addr = iptr->Indirect_ptr_lbas;
                no_of_data_block_ptrs = 510;
                data_block_lba_ptr = iptr->Direct_data_lbas;
                curr = 0;
            }
        }

        // Store dirty block to NVM
        if (iptr == NULL)
        {
            // addresses->push_back();
            Store_To_NVM(FSObj, 4096 + (ptr->Inode_no * INODE_SIZE), ptr, 4096);
        }
        else
        {
            Store_To_NVM(FSObj, iptr->Current_addr, iptr, 4096);
        }

        free(iptr);
        return 0;
    }

    // MYFS_File definition
    MYFS_File::MYFS_File(std::string filePath, MYFS *FSObj)
    {
        this->FSObj = FSObj;
        Get_Path_Inode(FSObj, filePath, &(this->ptr));
        this->curr_read_offset = 0;
    }

    int MYFS_File::PRead(uint64_t offset, uint64_t size, char *data)
    {
        if (ptr->FileSize < offset + size) {
            if(offset >= ptr->FileSize)
                return 0;
            size = ptr->FileSize - offset;
        } 

        std::vector<uint64_t> addresses_to_read;
        int err = get_blocks_addr(this->FSObj, this->ptr, offset, size, &addresses_to_read, false);
        if (err)
            return 0;
    
        char *readD = (char *)calloc(addresses_to_read.size(), 4096);
        for (int i = 0; i < addresses_to_read.size(); i++)
            Load_From_NVM(this->FSObj, addresses_to_read.at(i), readD + (i * 4096), 4096);

        int smargin = offset % 4096;
        memcpy(data, readD + smargin, size);
        free(readD);
        return size;
    }

    int MYFS_File::Read(uint64_t size, char *data)
    {
        // Check with file size
        int sizeW = this->PRead(this->curr_read_offset, size, data);
        this->curr_read_offset += sizeW;
        return sizeW;
    }

    int MYFS_File::Seek(uint64_t offset)
    {
        if (ptr->FileSize < this->curr_read_offset + offset)
            return -1;
        this->curr_read_offset += offset;
        return 0;
    }

    int MYFS_File::Truncate(uint64_t size)
    {
        // TODO: Free Data Block
        this->ptr->FileSize = size;
        return 0;
    }

    int MYFS_File::PAppend(uint64_t offset, uint64_t size, char *data)
    {
        std::vector<uint64_t> addresses_to_read;
        int err = get_blocks_addr(this->FSObj, this->ptr, offset, size, &addresses_to_read, false);
        if (err)
            return -1;

        // Do read-modify-update cycle if smargin is present on 1st address.
        int smargin = offset % 4096;
        char *buffer = (char *)calloc(addresses_to_read.size(), 4096);
        if (smargin)
            Load_From_NVM(this->FSObj, addresses_to_read.at(0), buffer, 4096);

        memcpy(buffer + smargin, data, size);
        for (int i = 0; i < addresses_to_read.size(); i++)
            Store_To_NVM(this->FSObj, addresses_to_read.at(i), data + (i * 4096), 4096);

        // Update file size
        this->ptr->FileSize = offset + size;
        free(buffer);
        return 0;
    }

    int MYFS_File::Append(uint64_t size, char *data)
    {
        return this->PAppend(this->ptr->FileSize, size, data);
    }

    uint64_t MYFS_File::GetFileSize()
    {
        return this->ptr->FileSize;
    }

    int MYFS_File::Close()
    {
        // Flush Inode changes to Disk
    }

    // Def of MYFS_SequentialFile
    MYFS_SequentialFile::MYFS_SequentialFile(std::string fpath, MYFS *FSObj)
    {
        this->fp = new MYFS_File(fpath, FSObj);
    }

    IOStatus MYFS_SequentialFile::Read(size_t n, const IOOptions &opts, Slice *result, char *scratch, IODebugContext *dbg)
    {
        
        int sizeW = this->fp->Read(n, scratch);
        *result = Slice(scratch, sizeW);
        return IOStatus::OK();
    }

    // IOStatus MYFS_SequentialFile::PositionedRead(uint64_t offset, size_t n, const IOOptions &opts, Slice *result,
    //                                              char *scratch, IODebugContext *dbg)
    // {
    //     int err = this->fp->PRead(offset, n, scratch);
    //     if (err)
    //         return IOStatus::IOError(__FUNCTION__);
    //     *result = Slice(scratch, n);
    //     return IOStatus::OK();
    // }

    IOStatus MYFS_SequentialFile::Skip(uint64_t n)
    {
        int err = this->fp->Seek(n);
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }

    // Def MYFS_RandomAccessFile
    MYFS_RandomAccessFile::MYFS_RandomAccessFile(std::string fname, MYFS *FSObj)
    {
        this->fp = new MYFS_File(fname, FSObj);
    }

    IOStatus MYFS_RandomAccessFile::Read(uint64_t offset, size_t n, const IOOptions &opts, Slice *result, char *scratch,
                                         IODebugContext *dbg) const
    {
        int sizeW = this->fp->PRead(offset, n, scratch);
        *result = Slice(scratch, sizeW);
        return IOStatus::OK();
    }

    // Def MYFS_WritableFile
    MYFS_WritableFile::MYFS_WritableFile(std::string fname, MYFS *FSObj)
    {
        this->fp = new MYFS_File(fname, FSObj);
        this->cache = false;
        this->cacheSize = 0;
    }

    IOStatus MYFS_WritableFile::Truncate(uint64_t size, const IOOptions &opts, IODebugContext *dbg)
    {
        int err = this->fp->Truncate(size);
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
    }

    IOStatus MYFS_WritableFile::ClearCache() {
        if(!this->cache)
            return IOStatus::OK();
        this->cache = false;
        int err = this->fp->Append(this->cacheSize, this->cacheData);
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        free(this->cacheData);
        this->cacheSize = 0;
        return IOStatus::OK();
    }

    IOStatus MYFS_WritableFile::Append(const Slice &data, const IOOptions &opts, IODebugContext *dbg)
    {
        
        char *block = (char *)data.data();
        uint64_t size = data.size();
        if(this->cache) {
            //Append to cache
            char *tmp = (char *)calloc(1, this->cacheSize+size);
            memcpy(tmp, this->cacheData, this->cacheSize);
            memcpy(tmp+this->cacheSize, block, size);
            free(this->cacheData);
            this->cacheData = tmp;
            this->cacheSize += size;
            //If size > 4096 clear cache
            if(this->cacheSize >= 4096)
                this->ClearCache();
            return IOStatus::OK();
        } else if(size < 4096) {
            //Append to cache
            this->cache = true;
            this->cacheData = (char *)calloc(1, size);
            memcpy(this->cacheData, block, size);
            this->cacheSize = size;
            return IOStatus::OK();
        }
        int err = this->fp->Append(size, block);
        if (err)
            return IOStatus::IOError(__FUNCTION__);
        return IOStatus::OK();
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
