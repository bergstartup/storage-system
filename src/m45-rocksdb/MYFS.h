#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

#include "./MYFS_IO.h"
#include <iostream>


namespace MYFS {
    //Function declarations
    void Get_ParentPath(std::string path, std::string &parent);
    void Get_EntityName(std::string path, std::string &entityName);
    void Clean_Path(std::string path, std::string &newPath);

    uint32_t Load_Children(MYFS_META *FSObj, Inode *ptr, std::string entityName, std::vector<std::string> *children, bool loadChildren, std::string targetName = "");
    int Get_Path_Inode(MYFS_META *FSObj, std::string path, Inode **ptr);
    int Rename_Child_In_Parent(MYFS_META *FSObj, std::string Ppath, std::string targetName, std::string srcName);
    int Update_Parent(MYFS_META *FSObj, std::string Ppath, std::string childName, uint32_t childInode, bool del = false);
    
    int MYFS_CreateFile(MYFS_META *FSObj, std::string path);
    int MYFS_CreateDir(MYFS_META *FSObj, std::string path); 
    



    //Class declarations
    class MYFS_File
    {
        private:
            struct Inode *ptr;
            MYFS_META *FSObj;
            uint64_t curr_read_offset;
            void *current_ptr;

        public:
            MYFS_File(std::string filePath, MYFS_META *FSObj);
            virtual ~MYFS_File() = default;
            int Read(uint64_t size, char *data);
            int PRead(uint64_t offset, uint64_t size, char *data);
            int Seek(uint64_t offset);
            int Truncate(uint64_t size);
            int Append(uint64_t size, char *data);
            int PAppend(uint64_t offset, uint64_t size, char *data);
            uint64_t GetFileSize();
            int Close();
    };


    class MYFS_SequentialFile : public ROCKSDB_NAMESPACE::FSSequentialFile
    {
    private:
        MYFS_File *fp;

    public:
        MYFS_SequentialFile(std::string filePath, MYFS_META *FSObj);
        virtual ~MYFS_SequentialFile(){delete this->fp;}
        virtual ROCKSDB_NAMESPACE::IOStatus Read(size_t n, const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::Slice *result,
                              char *scratch, ROCKSDB_NAMESPACE::IODebugContext *dbg)override;
       
        virtual ROCKSDB_NAMESPACE::IOStatus Skip(uint64_t n) override;
        // virtual IOStatus PositionedRead(uint64_t offset, size_t n,
        //                                 const IOOptions &opts, Slice *result,
        //                                 char *scratch, IODebugContext *dbg) override;
        // virtual IOStatus InvalidateCache(size_t offset, size_t length) override
        // {
        //     return IOStatus::OK();
        // };
        // virtual bool use_direct_io() const override { return true; }
        // virtual size_t GetRequiredBufferAlignment() const override { return 4096; }
    };

    class MYFS_RandomAccessFile : public ROCKSDB_NAMESPACE::FSRandomAccessFile
    {
    private:
        MYFS_File *fp;

    public:
        MYFS_RandomAccessFile(std::string fname, MYFS_META *FSObj);
        virtual ~MYFS_RandomAccessFile(){delete this->fp;}
        virtual ROCKSDB_NAMESPACE::IOStatus Read(uint64_t offset, size_t n, const ROCKSDB_NAMESPACE::IOOptions &opts,
                              ROCKSDB_NAMESPACE::Slice *result, char *scratch, ROCKSDB_NAMESPACE::IODebugContext *dbg) const override;
        /*
        virtual IOStatus MultiRead(FSReadRequest *reqs, size_t num_reqs,
                                   const IOOptions &options,
                                   IODebugContext *dbg) {std::cout<<"MULTIREAD"<<std::endl;return IOStatus::OK();}

        virtual IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions &opts,
                                  IODebugContext *dbg) {std::cout<<"PRE-FETCH"<<std::endl;return IOStatus::OK();}

        virtual IOStatus InvalidateCache(size_t offset, size_t length) override { return IOStatus::OK(); };
        virtual bool use_direct_io() const override { return true; }
        virtual size_t GetRequiredBufferAlignment() const override { return 4096; }
        */
    };

    class MYFS_WritableFile : public ROCKSDB_NAMESPACE::FSWritableFile
    {
    private:
        MYFS_File *fp;
        bool cache;
        uint64_t cacheSize;
        char *cacheData;
        virtual ROCKSDB_NAMESPACE::IOStatus ClearCache();
    public:
        MYFS_WritableFile(std::string fname, MYFS_META *FSObj);
        virtual ~MYFS_WritableFile(){this->ClearCache();delete this->fp;}
        virtual ROCKSDB_NAMESPACE::IOStatus Truncate(uint64_t size, const ROCKSDB_NAMESPACE::IOOptions &opts,
                                  ROCKSDB_NAMESPACE::IODebugContext *dbg) override;
        virtual ROCKSDB_NAMESPACE::IOStatus Close(const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::IODebugContext *dbg) {return ROCKSDB_NAMESPACE::IOStatus::OK();};
        virtual ROCKSDB_NAMESPACE::IOStatus Append(const ROCKSDB_NAMESPACE::Slice &data, const ROCKSDB_NAMESPACE::IOOptions &opts,
                                ROCKSDB_NAMESPACE::IODebugContext *dbg) override;
        virtual ROCKSDB_NAMESPACE::IOStatus Flush(const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::IODebugContext *dbg) override { return ROCKSDB_NAMESPACE::IOStatus::OK(); }
        virtual ROCKSDB_NAMESPACE::IOStatus Sync(const ROCKSDB_NAMESPACE::IOOptions &opts, ROCKSDB_NAMESPACE::IODebugContext *dbg) override { return ROCKSDB_NAMESPACE::IOStatus::OK(); }
        /*
        virtual IOStatus Append(const Slice &data, const IOOptions &opts,
                                const DataVerificationInfo & /* verification_info ,
                                IODebugContext *dbg) override
        {
            return Append(data, opts, dbg);
        }
        virtual IOStatus PositionedAppend(const Slice &data, uint64_t offset,
                                          const IOOptions &opts,
                                          IODebugContext *dbg) override;
        virtual IOStatus PositionedAppend(const Slice &data, uint64_t offset,
                                          const IOOptions &opts, const DataVerificationInfo & /* verification_info,
                                          IODebugContext *dbg) override
        {
            return PositionedAppend(data, offset, opts, dbg);
        }
        
        virtual IOStatus Fsync(const IOOptions &opts, IODebugContext *dbg) override { return IOStatus::OK(); }
        virtual bool IsSyncThreadSafe() const { return false; }
        virtual bool use_direct_io() const override { return true; }
        virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {}
        virtual uint64_t GetFileSize(const IOOptions &opts,
                                     IODebugContext *dbg) override {std::cout<<"Calling this module"<<std::endl;;return this->fp->GetFileSize();}
        virtual IOStatus InvalidateCache(size_t offset, size_t length) override { return IOStatus::OK(); }
        virtual size_t GetRequiredBufferAlignment() const override { return 4096; }
        */
    };

    class MYFS_Directory : public ROCKSDB_NAMESPACE::FSDirectory
    {
            public:
            MYFS_Directory(MYFS_META *FSObj){}
            virtual ~MYFS_Directory(){}
            virtual ROCKSDB_NAMESPACE::IOStatus Fsync(const ROCKSDB_NAMESPACE::IOOptions& opts, ROCKSDB_NAMESPACE::IODebugContext* dbg) override {
                return ROCKSDB_NAMESPACE::IOStatus::OK();
            }
    };
}