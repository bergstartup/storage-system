#include <string>
#include <iostream>
#include "MYFS.h"


namespace MYFS {
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
    uint32_t Load_Children(MYFS_META *FSObj, Inode *ptr, std::string entityName, std::vector<std::string> *children, bool loadChildren, std::string targetName = "")
    {
        
        // Check no of children and load it
        uint64_t children_count = ptr->FileSize;

        MYFS_Dir *dir_ptr = (MYFS_Dir *)calloc(1, sizeof(MYFS_Dir));
        for (int i = 0; i < children_count / 16; i++)
        {
            Load_From_NVM(FSObj, ptr->Direct_data_lbas[i], dir_ptr, 4096);
            for (int j = 0; j < 16; j++)
            {
                if (loadChildren)
                    children->push_back(dir_ptr->Entities[j].EntityName);
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
            if (loadChildren)
                children->push_back(dir_ptr->Entities[i].EntityName);
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
    int Get_Path_Inode(MYFS_META *FSObj, std::string path, Inode **ptr)
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
        ptr = (Inode **)calloc(1, sizeof(Inode));
        isPresent = Load_From_NVM(FSObj, address, ptr, (uint64_t)INODE_SIZE);
        if (!isPresent)
            return -1;

        // Put it in lookup Map
        LookupMap_Insert(FSObj, path, *ptr);
        return 0;
    }

    int Rename_Child_In_Parent(MYFS_META *FSObj, std::string Ppath, std::string targetName, std::string srcName)
    {
        // FIXME: Logic for rename
        Inode *parentInode;
        int isPresent = Get_Path_Inode(FSObj, Ppath, &parentInode);
        uint32_t rename = Load_Children(FSObj, parentInode, srcName, NULL, false, targetName);
        return rename;
    }

    int Update_Parent(MYFS_META *FSObj, std::string Ppath, std::string childName, uint32_t childInode, bool del = false)
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
    

    int MYFS_CreateFile(MYFS_META *FSObj, std::string path)
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

    int MYFS_CreateDir(MYFS_META *FSObj, std::string path)
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

    /*--------------------CLASS DEFS------------------------*/
    // MYFS_File definition
    MYFS_File::MYFS_File(std::string filePath, MYFS_META *FSObj)
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
            return -1;
    
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
    MYFS_SequentialFile::MYFS_SequentialFile(std::string fpath, MYFS_META *FSObj)
    {
        this->fp = new MYFS_File(fpath, FSObj);
    }

    ROCKSDB_NAMESPACE::IOStatus MYFS_SequentialFile::Read(size_t n, const ROCKSDB_NAMESPACE::IOOptions &opts,ROCKSDB_NAMESPACE::Slice *result, char *scratch, ROCKSDB_NAMESPACE::IODebugContext *dbg)
    {
        
        int sizeW = this->fp->Read(n, scratch);
        *result = ROCKSDB_NAMESPACE::Slice(scratch, sizeW);
        return ROCKSDB_NAMESPACE::IOStatus::OK();
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

    ROCKSDB_NAMESPACE::IOStatus MYFS_SequentialFile::Skip(uint64_t n)
    {
        int err = this->fp->Seek(n);
        if (err)
            return ROCKSDB_NAMESPACE::IOStatus::IOError(__FUNCTION__);
        return ROCKSDB_NAMESPACE::IOStatus::OK();
    }

    // Def MYFS_RandomAccessFile
    MYFS_RandomAccessFile::MYFS_RandomAccessFile(std::string fname, MYFS_META *FSObj)
    {
        this->fp = new MYFS_File(fname, FSObj);
    }

    ROCKSDB_NAMESPACE::IOStatus MYFS_RandomAccessFile::Read(uint64_t offset, size_t n, const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::Slice *result, char *scratch,
                                         ROCKSDB_NAMESPACE::IODebugContext *dbg) const
    {
        int sizeW = this->fp->PRead(offset, n, scratch);
        *result = ROCKSDB_NAMESPACE::Slice(scratch, sizeW);
        return ROCKSDB_NAMESPACE::IOStatus::OK();
    }

    // Def MYFS_WritableFile
    MYFS_WritableFile::MYFS_WritableFile(std::string fname, MYFS_META *FSObj)
    {
        this->fp = new MYFS_File(fname, FSObj);
        this->cache = false;
        this->cacheSize = 0;
    }

    ROCKSDB_NAMESPACE::IOStatus MYFS_WritableFile::Truncate(uint64_t size, const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::IODebugContext *dbg)
    {
        int err = this->fp->Truncate(size);
        if (err)
            return ROCKSDB_NAMESPACE::IOStatus::IOError(__FUNCTION__);
        return ROCKSDB_NAMESPACE::IOStatus::OK();
    }

    ROCKSDB_NAMESPACE::IOStatus MYFS_WritableFile::ClearCache() {
        if(!this->cache)
            return ROCKSDB_NAMESPACE::IOStatus::OK();
        this->cache = false;
        int err = this->fp->Append(this->cacheSize, this->cacheData);
        if (err)
            return ROCKSDB_NAMESPACE::IOStatus::IOError(__FUNCTION__);
        free(this->cacheData);
        this->cacheSize = 0;
        return ROCKSDB_NAMESPACE::IOStatus::OK();
    }

    ROCKSDB_NAMESPACE::IOStatus MYFS_WritableFile::Append(const ROCKSDB_NAMESPACE::Slice &data, const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::IODebugContext *dbg)
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
            if(this->cacheSize >= 4096*200)
                this->ClearCache();
            return ROCKSDB_NAMESPACE::IOStatus::OK();
        } else if(size < 4096*200) {
            //Append to cache
            this->cache = true;
            this->cacheData = (char *)calloc(1, size);
            memcpy(this->cacheData, block, size);
            this->cacheSize = size;
            return ROCKSDB_NAMESPACE::IOStatus::OK();
        }
        int err = this->fp->Append(size, block);
        if (err)
            return ROCKSDB_NAMESPACE::IOStatus::IOError(__FUNCTION__);
        return ROCKSDB_NAMESPACE::IOStatus::OK();
    }


}