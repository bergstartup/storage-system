#include <string>
#include <utils.h>
#include <sys/mman.h>
#include <vector>
#include "./MYFS_IO.h"

namespace MYFS {
    int Load_From_NVM(MYFS_META *FSObj, uint64_t addr, void *buffer, uint64_t size)
    {
        // Check the size if quantization of LBA
        int err = zns_udevice_read(FSObj->zns, addr, buffer, size);
        return 0;
    }

    int Store_To_NVM(MYFS_META *FSObj, uint64_t addr, void *buffer, uint64_t size)
    {
        int err = zns_udevice_write(FSObj->zns, addr, buffer, size);
        return 0;
    }


    int LookupMap_HashFunction(std::string id)
    {
        unsigned hashindex;
        char *ptr = const_cast<char *>(id.c_str());
        for (hashindex = 0; *ptr != '\0'; ptr++)
            hashindex = *ptr + STRINGENCODE * hashindex;
        return hashindex % LOOKUP_MAP_SIZE;
    }

    int LookupMap_Insert(MYFS_META *FSObj, std::string id, Inode *ptr)
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

    int LookupMap_Delete(MYFS_META *FSObj, std::string id)
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

    int LookupMap_Lookup(MYFS_META *FSObj, std::string id, Inode **ptr)
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


    uint32_t get_FreeInode(MYFS_META *FSObj)
    {
        uint32_t ptr = (FSObj->InodePtr + 1) % MAX_INODE_COUNT;
        while (ptr != FSObj->InodePtr)
        {
            if (!FSObj->InodeBitMap[ptr])
            {
                FSObj->InodePtr = ptr;
                return ptr;
            }
            ptr = (ptr + 1) % MAX_INODE_COUNT;
        }
        return 0;
    }

    uint64_t get_FreeDataBlock(MYFS_META *FSObj)
    {
        uint64_t ptr = (FSObj->DataBlockPtr + 1) % FSObj->DataBlockCount;
        while (ptr != FSObj->DataBlockPtr)
        {
            if (!FSObj->DataBitMap[ptr])
            {
                FSObj->DataBlockPtr = ptr;
                return (ptr + DATA_BLOCKS_OFFSET) * FSObj->LogicalBlockSize;
            }
            ptr = (ptr + 1) % FSObj->DataBlockCount;
        }
        return 0;
    }

    void free_DataBlock(MYFS_META *FSObj, uint64_t addr)
    {
        int index = (addr / FSObj->LogicalBlockSize) - DATA_BLOCKS_OFFSET;
        FSObj->DataBitMap[index] = false;
    }

    int get_blocks_addr(MYFS_META *FSObj, Inode *ptr, uint64_t offset, uint64_t size, std::vector<uint64_t> *addresses, bool forWrite)
    {
        uint32_t curr = offset / 4096, end = (offset+size) / 4096;
        uint64_t if_dirty_addr;
        uint64_t *data_block_lba_ptr, next_indirect_block_addr;
        uint32_t no_of_data_block_ptrs;
        MYFS_Indirect_Ptr_Block *iptr = NULL;
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
            iptr = (MYFS_Indirect_Ptr_Block *)calloc(1, 4096);
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
                    iptr = (MYFS_Indirect_Ptr_Block *)calloc(1, 4096);
                    iptr->Current_addr = next_indirect_block_addr;
                }
                else
                {
                    if (iptr == NULL)
                        iptr = (MYFS_Indirect_Ptr_Block *)calloc(1, 4096);

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
}
