#include <zns_device.h>
#include <iostream>
#include <string>

#define LOOKUP_MAP_SIZE 1000
#define MAX_INODE_COUNT 255
#define INODE_SIZE 4096
#define SUPER_BLOCK_SIZE 4096
#define STRINGENCODE 31
#define DATA_BLOCKS_OFFSET 256

namespace MYFS {
    struct Inode
    {
        uint32_t Inode_no;
        char EntityName[235];
        bool IsDir;
        uint64_t FileSize;
        uint64_t Indirect_ptr_lbas;
        uint64_t Direct_data_lbas[480];
    };

    struct MYFS_Indirect_Ptr_Block
    {
        uint64_t Current_addr;
        uint64_t Direct_data_lbas[510];
        uint64_t Indirect_ptr_lbas;
    };

    struct MYFS_DirData
    {
        char EntityName[252];
        uint32_t InodeNum;
    };

    struct MYFS_Dir
    {
        MYFS_DirData Entities[16];
    };

    struct mapEntries
    {
        char id[1000];
        Inode *ptr;
        mapEntries *chain;
    };

    struct MYFS_META
    {
        mapEntries *LookupCache[LOOKUP_MAP_SIZE]; // Map type to void ptrs;
        bool InodeBitMap[MAX_INODE_COUNT];
        bool *DataBitMap;
        uint32_t InodePtr;

        uint64_t DataBlockPtr;
        uint64_t DataBlockMax;

        uint64_t DataBlockCount;
        uint64_t FileSystemCapacity;
        uint32_t LogicalBlockSize;
        Inode *rootEntry;
        user_zns_device *zns;
    };
    
    int Load_From_NVM(MYFS_META *FSObj, uint64_t addr, void *buffer, uint64_t size);
    int Store_To_NVM(MYFS_META *FSObj, uint64_t addr, void *buffer, uint64_t size);

    int LookupMap_HashFunction(std::string id);
    int LookupMap_Insert(MYFS_META *FSObj, std::string id, Inode *ptr);
    int LookupMap_Delete(MYFS_META *FSObj, std::string id);
    int LookupMap_Lookup(MYFS_META *FSObj, std::string id, Inode **ptr);

    uint64_t get_FreeDataBlock(MYFS_META *FSObj);
    uint32_t get_FreeInode(MYFS_META *FSObj);
    int get_blocks_addr(MYFS_META *FSObj, Inode *ptr, uint64_t offset, uint64_t size, std::vector<uint64_t> *addresses, bool forWrite);
}