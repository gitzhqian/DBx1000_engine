#pragma once 

#include <cassert>
#include <atomic>
#include <unordered_map>
#include <memory>
#include "global.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		value = *(type *)&data[pos];\
	}

class table_t;
class Catalog;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_peloton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;

class row_m{
public:
    //64 bits
    uint64_t meta;

    ~row_m() {};

    row_m() : meta(0) {}
    explicit row_m(uint64_t meta) : meta(meta)  {}
    //1.1-TxnMask inserting,updating 0-TxnMask not inserting / updating
    //2.1-visible, 0-not visible
    //3.key length
    //4.record offset
    //5.TxnContext,
    //  commit id of the txn that creats the record version
    static const uint64_t kControlMask = uint64_t{0x1} << 63;          // Bits 64
    static const uint64_t kVisibleMask = uint64_t{0x1} << 62;          // Bit 63
    static const uint64_t kKeyLengthMask = uint64_t{0x2FFF} << 48;     // Bits 62-49
    static const uint64_t kOffsetMask = uint64_t{0xFFFF} << 32;        // Bits 48-33
    static const uint64_t kTxnContextMask = uint64_t{0xFFFFFFFF};      // Bits 32-1


    bool IsNull() const {
        return (meta == std::numeric_limits<uint64_t>::max() && meta == 0);
    }

    bool operator==(const row_m &rhs) const {
        return meta == rhs.meta ;
    }

    bool operator!=(const row_m &rhs) const {
        return !operator==(rhs);
    }

    inline bool IsVacant() { return meta == 0; }

    inline bool TxnContextIsRead() const {
        bool t_i_r = (meta & kControlMask) > 0;
        return t_i_r;
    }

    inline uint16_t GetKeyLength() const { return (uint16_t) ((meta & kKeyLengthMask) >> 48); }

    // Get the padded key length from accurate key length
    inline uint16_t GetPaddedKeyLength() const{
        auto key_length = GetKeyLength();
        return PadKeyLength(key_length);
    }

    static inline constexpr uint16_t  PadKeyLength(uint16_t key_length) {
        return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
    }

    inline uint16_t GetOffset() { return (uint16_t) ((meta & kOffsetMask) >> 32); }

    inline void SetOffset(uint16_t offset) {
        meta = (meta & (~kOffsetMask)) | (uint64_t{offset} << 32);
    }

    inline bool IsVisible() {
        bool i_v = (meta & kVisibleMask) > 0;
        return i_v;
    }

    //when the record is deleted, visible=false
    //1-visible, 0-not visible
    inline void SetVisible(bool visible) {
        if (visible) {
            meta = meta | kVisibleMask;
        } else {
            meta = meta & (~kVisibleMask);
        }
    }

    inline void PrepareForInsert(uint64_t offset, uint64_t key_len, uint32_t comm_id) {
        // visible but inserting
        meta =  (key_len << 48) | (offset << 32) | comm_id;
        meta =  meta | (kControlMask) | (kVisibleMask) ;

        assert(IsInserting());
    }

    inline void PrepareForUpdate() {
        meta =  meta | (kControlMask) | (kVisibleMask) ;

        assert(IsInserting());
    }

    inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint32_t commit_id) {
        // Set the visible bit,the actual offset,key length, transaction commit id
        meta =  (key_len << 48) | (uint64_t{1} << 62)| (offset << 32);
        meta = meta | commit_id;
        meta = meta & (~kControlMask);

        auto g_k_l = GetKeyLength();

        assert(g_k_l == key_len);
        assert(!IsInserting());
    }

    inline void FinalizeForInsert(uint32_t commit_id) {
        meta = (meta & (~kTxnContextMask)) | commit_id;
        meta =  meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForUpdate(uint32_t sstamp) {
        meta = (meta & (~kTxnContextMask)) | sstamp;
        meta = meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForUpdate() {
        meta =  (meta & (~kControlMask)) | (kVisibleMask) ;

        assert(!IsInserting());
        assert(IsVisible());
    }

    inline void FinalizeForRead(uint32_t pstamp) {
        meta = (meta & (~kTxnContextMask)) | pstamp;
        meta = meta & (~kControlMask);

        assert(!IsInserting());
    }

    inline void FinalizeForDelete() {
        // Set the transaction commit id
        meta = 0;
        assert(!IsInserting());
    }

    inline void NotInserting() {
        meta = meta | (uint64_t{1} << 63);
    }

    inline bool IsInserting() {
        // record is not deleted and is writing
        return (IsVisible()) && TxnContextIsRead();
    }

    //create timestamp of the tuple latest version cstamp
    inline uint32_t GetTxnCommitId() const {
        return (uint32_t) (meta & kTxnContextMask);
    }
};

class row_t{
public:

//    row_t():data(nullptr),location(nullptr),manager(nullptr), key_size(0), version_t(0){};
//    row_t(uint8_t version_t): version_t(0){ };

	RC init(table_t * host_table, uint64_t part_id, uint64_t row_id = 0, uint32_t tuple_size = 0);
    void init(int size );
	void init(int size, row_t *src);
    void init_insrt(table_t * table_, uint64_t part_id, uint64_t row_id);
	RC switch_schema(table_t * host_table);
	// not every row has a manager
	void init_manager(row_t * row);

	table_t * get_table();
	Catalog * get_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();

	void copy(row_t * src);

	void 		set_primary_key(uint64_t key) { _primary_key = key; };
    void 		set_part_id(uint64_t part_id) { _part_id = part_id; };
    void 		set_row_id(uint64_t row_id) { _row_id = row_id; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };
    uint64_t    get_row_id() { return _row_id; };

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_value(int id);
	char * get_value(char * col_name);
	
	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);


	void set_data(char * data, uint64_t size);
	char * get_data();

	void free_row();

	// for concurrency control. can be lock, timestamp etc.
	RC get_row(access_t type, txn_man * txn, row_t *& row, ts_t & row_latest, uint64_t &read_addr);
	void return_row(access_t type, txn_man * txn, row_t * row);
	
    uint64_t  _primary_key;
    uint64_t  _part_id;
    uint64_t  _row_id;
    row_ops   is_valid = INVALID;
    table_t   * table;
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
#elif CC_ALG == TIMESTAMP
    Row_ts * manager;
#elif CC_ALG == MVCC
    Row_mvcc * manager;
#elif CC_ALG == HEKATON
    Row_hekaton * manager;
#elif CC_ALG == PELOTON
    Row_peloton * manager;
#elif CC_ALG == OCC
    Row_occ * manager;
  #elif CC_ALG == TICTOC
    Row_tictoc * manager;
  #elif CC_ALG == SILO
    Row_silo * manager;
  #elif CC_ALG == VLL
    Row_vll * manager;
#endif
    char      * data;
    //    void * location; //pointing to the location that holds the realtime location
//    row_t * next;

private:
	// primary key should be calculated from the data stored in the row.
//	uint64_t 		_primary_key;
//	uint64_t		_part_id;
//	uint64_t 		_row_id;

};

//
//class DualPointer {
//public:
//    // pointing to the row_t's realtime location
//    uint64_t row_t_location;
//
//    // pointing to the latest version
//    uint64_t latest_version;
//
//    DualPointer() : row_t_location(0), latest_version(0) {}
//
//    DualPointer(uint64_t r_t_ptr, uint64_t l_v_ptr) : row_t_location(r_t_ptr), latest_version(l_v_ptr) {}
//
//    bool IsNull() const {
//        return (row_t_location ==0 && latest_version == 0);
//    }
//
//    bool operator==(const DualPointer &rhs) const {
//        return ( row_t_location == rhs.row_t_location && latest_version == rhs.latest_version);
//    }
//
//
//} __attribute__((__aligned__(8))) __attribute__((__packed__));
//
//extern DualPointer INVALID_DUALPOINTER;
//const size_t INVALID_DUAL_POINTER_ARRAY_OFFSET = std::numeric_limits<size_t>::max();
//class DualPointerArray {
//public:
//    DualPointerArray(uint32_t id) : id_(id) {
//        dual_pointers_.reset(new dual_pointer_array_());
//    }
//
//    ~DualPointerArray() {}
//
//    size_t AllocateDualPointerArray() {
//        if (counter_ >= DURAL_POINTER_ARRAY_MAX_SIZE) {
//            return INVALID_DUAL_POINTER_ARRAY_OFFSET;
//        }
//
//        size_t indirection_id =
//                counter_.fetch_add(1, std::memory_order_relaxed);
//
//        if (indirection_id >= DURAL_POINTER_ARRAY_MAX_SIZE) {
//            return INVALID_DUAL_POINTER_ARRAY_OFFSET;
//        }
//        return indirection_id;
//    }
//    DualPointer *GetIndirectionByOffset(const size_t &offset) {
//        return &(dual_pointers_->at(offset));
//    }
//    inline uint32_t GetId() { return id_; }
//
//private:
//    typedef std::array<DualPointer, DURAL_POINTER_ARRAY_MAX_SIZE> dual_pointer_array_;
//    std::unique_ptr<dual_pointer_array_> dual_pointers_;
//    std::atomic<size_t> counter_ = ATOMIC_VAR_INIT(0);
//    uint32_t id_;
//};
//
//class DuralPointerManager {
//public:
//    DuralPointerManager() {}
//    // Singleton
//    static DuralPointerManager &GetInstance(){
//        static DuralPointerManager dual_pointer_manager;
//        return dual_pointer_manager;
//    }
//    uint32_t GetNextDulaPointerArrayId() { return ++dual_pointer_array_id_; }
//    uint32_t GetCurrentDualPointerArrayId() { return dual_pointer_array_id_; }
//    void AddDualPointerArray(const uint32_t id,
//                             std::shared_ptr<DualPointerArray> location){
//        auto ret = dual_pointer_array_locator_[id] = location;
//    }
//    void DropDualPointerArray(const uint32_t id){
//        dual_pointer_array_locator_[id] = dual_pointer_empty_array_;
//    }
//    void ClearDualPointerArray(){
//        dual_pointer_array_locator_.clear();
//    }
//
//private:
//    std::atomic<uint32_t> dual_pointer_array_id_ = ATOMIC_VAR_INIT(0);
//    std::unordered_map<uint32_t, std::shared_ptr<DualPointerArray>> dual_pointer_array_locator_;
//    static std::shared_ptr<DualPointerArray> dual_pointer_empty_array_;
//};
