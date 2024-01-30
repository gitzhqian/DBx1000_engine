#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"

RC tpcc_wl::init() {
	workload::init();
//	string path = "./benchmarks/";
    string path = "/home/zhangqian/papers/hdbms/HDBMSengine/benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCC schema initialized" << endl;
	init_table();
	next_tid = 0;
	return RCOK;
}

RC tpcc_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_district_ext = tables["DISTRICT-EXT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_district_ext = indexes["DISTRICT_EXT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
//    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];
    i_order = indexes["ORDER_IDX"];
//    i_order_cust = indexes["ORDER_CUST_IDX"];
    i_neworder = indexes["NEWORDER_IDX"];
    i_orderline = indexes["ORDERLINE_IDX"];

    tables_[0] = t_item;
    tables_[1] = t_warehouse;
    tables_[2] = t_district;
    tables_[3] = t_district_ext;
    tables_[4] = t_customer;
    tables_[5] = t_history;
    tables_[6] = t_stock;
    tables_[7] = t_order;
    tables_[8] = t_orderline;
    tables_[9] = t_neworder;
    tables_[10] = NULL;

    indexes_[0] = i_item;
    indexes_[1] = i_warehouse;
    indexes_[2] = i_district;
    indexes_[3] = i_district_ext;
    indexes_[4] = i_customer_id;
//    indexes_[5] = i_customer_last;
    indexes_[6] = i_stock;
    indexes_[7] = i_order;
//    indexes_[8] = i_order_cust;
    indexes_[9] = i_neworder;
    indexes_[10] = i_orderline;
    indexes_[11] = NULL;
    index_2_table_[0] = 0;
    index_2_table_[1] = 1;
    index_2_table_[2] = 2;
    index_2_table_[3] = 3;
    index_2_table_[4] = 4;
    index_2_table_[5] = 4;
    index_2_table_[6] = 6;
    index_2_table_[7] = 7;
    index_2_table_[8] = 7;
    index_2_table_[9] = 9;
    index_2_table_[10] = 8;
    return RCOK;
}

RC tpcc_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
    int buf_cnt = (num_wh > g_thread_cnt) ? num_wh : g_thread_cnt;
    tpcc_buffer = new drand48_data * [buf_cnt];
    for (uint32_t i = 0; i < buf_cnt; ++i) {
        // printf("%d\n", g_thread_cnt);
        tpcc_buffer[i] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
        srand48_r(i + 1, tpcc_buffer[i]);
    }
//    pthread_t * p_thds = new pthread_t[g_num_wh - 1];
//    for (uint32_t i = 0; i < g_num_wh - 1; i++)
//        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
//    threadInitWarehouse(this);
//    for (uint32_t i = 0; i < g_num_wh - 1; i++)
//        pthread_join(p_thds[i], NULL);

    tpcc_wl* wl = (tpcc_wl*)this;
    wl->init_tab_item();
    for (int i = 0; i < g_num_wh; ++i) {
        threadInitWarehouse(this);
    }

#if QUERY_2
    wl->init_tab_nation();
    wl->init_tab_region();
    wl->init_tab_supplier();
    // value ranges 0 ~ 9999 ( modulo by 10k )
    supp_stock_map.resize(10000);
    // pre-build supp-stock mapping table to boost tpc-ch queries
    for (uint w = 1; w <= g_num_wh; w++){
        for (uint i = 1; i <= g_max_items; i++){
            supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));
        }
    }


#endif

	printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}
void tpcc_wl::init_tab_region() {
    for (uint64_t i = 0; i < 5; ++i) {
        uint64_t r_id = i;
        std::string r_name = std::string(regions[i]);
        std::string r_comment = GetRandomAlphaNumericString(152);

        Region *region = new Region{r_id, r_name, r_comment};
        ch_regions.insert(std::make_pair(r_id, region));
    }
}
void tpcc_wl::init_tab_nation() {
    for (uint64_t i = 0; i < 62; i++) {
        uint64_t n_id = i;
        uint64_t r_id = nations[n_id].region_id;
        std::string n_name = nations[n_id].nation_name;
        std::string n_comment = GetRandomAlphaNumericString(152);

        Nation *nation = new Nation{n_id, r_id, n_name, n_comment};
        ch_nations.insert(std::make_pair(n_id, nation));
    }
}
void tpcc_wl::init_tab_supplier() {
    for (uint64_t i = 0; i < 10000; ++i) {
        uint64_t supp_id = i;
        uint64_t su_nation_id = GetRandomInteger(0, 61);
        double su_acctbal = GetRandomDouble(0.0, 0.2);
        std::string su_name = std::string("Supplier#") + std::string("000000000") + std::to_string(supp_id);;
        std::string su_address = GetRandomAlphaNumericString(40);; //40
        std::string su_phone = GetRandomAlphaNumericString(15);;  //15
        std::string su_comment = GetRandomAlphaNumericString(15);; //15

        Supplier *supplier = new Supplier{supp_id, su_nation_id, su_acctbal,
                                          su_name, su_address, su_phone, su_comment };
        ch_suppliers.insert(std::make_pair(supp_id, supplier));
    }
}

// TODO ITEM table is assumed to be in partition 0
void tpcc_wl::init_tab_item() {
	for (UInt32 i = 1; i <= g_max_items; i++) {
        row_t * row;
        uint64_t row_id;
        t_item->get_new_row(row, 0, row_id);
        row->set_primary_key(i);
        row->set_value(I_ID, i);
        row->set_value(I_PRICE, URand(1, 100, 0));
        row->set_value(I_IM_ID, URand(1L,10000L, 0));
        char name[24];
        MakeAlphaString(14, 24, name, 0);
        row->set_value(I_NAME, name);
        char data[50];
        int len = MakeAlphaString(26, 50, data, 0);
        // TODO in TPCC, "original" should start at a random position
        if (RAND(10, 0) == 0) {
            uint64_t startORIGINAL = URand(2, (len - 8), 0);
            strcpy(data + startORIGINAL, "original");
        }
        row->set_value(I_DATA, data);

        //memcpy( &data[pos], ptr, datasize);

		index_insert(i_item, i, row, 0, row_id);
	}
}

void tpcc_wl::init_tab_wh(uint32_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
    row_t * row;
    uint64_t row_id;
    t_warehouse->get_new_row(row, 0, row_id);
    row->set_primary_key(wid);

    row->set_value(W_ID, wid);
    char name[10];
    MakeAlphaString(6, 10, name, wid-1);
    row->set_value(W_NAME, name);
    char street[20];
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_CITY, street);
    char state[2];
    MakeAlphaString(2, 2, state, wid-1); /* State */
    row->set_value(W_STATE, state);
    char zip[9];
    MakeNumberString(9, 9, zip, wid-1); /* Zip */
    row->set_value(W_ZIP, zip);
    double tax = (double)URand(0L,200L,wid-1)/1000.0;
    double w_ytd=300000.00;
    row->set_value(W_TAX, tax);
    row->set_value(W_YTD, w_ytd);

    index_insert(i_warehouse, wid, row, wh_to_part(wid), row_id);
	return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
        uint64_t row_id;
        t_district->get_new_row(row, 0, row_id);
        row->set_primary_key(distKey(did, wid));

        row->set_value(D_ID, did);
        row->set_value(D_W_ID, wid);
        double tax = (double)URand(0L,200L,wid-1)/1000.0;
        double w_ytd=30000.00;
        row->set_value(D_TAX, tax);
        row->set_value(D_YTD, w_ytd);
        row->set_value(D_NEXT_O_ID, (int64_t)3001);
        char name[10];
        MakeAlphaString(6, 10, name, wid-1);
        row->set_value(D_NAME, name);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(D_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(D_ZIP, zip);

        index_insert(i_district, distKey(did, wid), row, wh_to_part(wid), row_id);
	}
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
	
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
        uint64_t row_id;
        t_stock->get_new_row(row, 0, row_id);
        uint64_t primary_key = stockKey(sid, wid);
        row->set_primary_key(primary_key);
        row->set_value(S_I_ID, sid);
        row->set_value(S_W_ID, wid);
        row->set_value(S_QUANTITY, (int64_t)URand(10, 100, wid-1));
        row->set_value(S_YTD, (int64_t)0);
        row->set_value(S_ORDER_CNT, (int64_t)0);
        row->set_value(S_REMOTE_CNT, (int64_t)0);
#if !TPCC_SMALL
        char s_dist[25];
        char row_name[10] = "S_DIST_";
        for (int i = 1; i <= 10; i++) {
            if (i < 10) {
                row_name[7] = '0';
                row_name[8] = i + '0';
            } else {
                row_name[7] = '1';
                row_name[8] = '0';
            }
            row_name[9] = '\0';
            MakeAlphaString(24, 24, s_dist, wid-1);
            row->set_value(row_name, s_dist);
        }
        char s_data[50];
        int len = MakeAlphaString(26, 50, s_data, wid-1);
        if (rand() % 100 < 10) {
            int idx = URand(0, len - 8, wid-1);
            strcpy(&s_data[idx], "original");
        }
        row->set_value(S_DATA, s_data);
#endif

        index_insert(i_stock, primary_key, row, wh_to_part(wid), row_id);
	}
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
        row_t * row;
        uint64_t row_id;
        t_customer->get_new_row(row, 0, row_id);

        row->set_value(C_ID, cid);
        row->set_value(C_D_ID, did);
        row->set_value(C_W_ID, wid);
        row->set_value(C_CREDIT_LIM, (int64_t)50000);
        double disc = URand(1, 5000, wid - 1) / 10000.0 ;
        row->set_value(C_DISCOUNT, disc);
        row->set_value(C_BALANCE, (double)(-10.0));
        row->set_value(C_YTD_PAYMENT, (double)(10.0));
        row->set_value(C_PAYMENT_CNT, (int64_t)1);
        row->set_value(C_DELIVERY_CNT, (int64_t)0);

        char c_last[LASTNAME_LEN];
        if (cid <= 1000)
            Lastname(cid - 1, c_last);
        else
            Lastname(NURand(255,0,999,wid-1), c_last);
        row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        row->set_value(C_MIDDLE, tmp);
        char c_first[FIRSTNAME_LEN];
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
        row->set_value(C_FIRST, c_first);
        char street[20];
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_CITY, street);
        char state[2];
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(C_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(C_ZIP, zip);
        char phone[16];
        MakeNumberString(16, 16, phone, wid-1); /* Zip */
        row->set_value(C_PHONE, phone);
        row->set_value(C_SINCE, 0);
        if (RAND(10, wid-1) == 0) {
            char tmp[] = "GC";
            row->set_value(C_CREDIT, tmp);
        } else {
            char tmp[] = "BC";
            row->set_value(C_CREDIT, tmp);
        }
        char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
        row->set_value(C_DATA, c_data);
#endif

        uint64_t key;
//        key = custNPKey(did, wid, c_last);
//        row->set_primary_key(key);
//        index_insert(i_customer_last, key, row, wh_to_part(wid));

        key = custKey(cid, did, wid);
        row_t *row_new;
        t_customer->get_new_row(row_new, 0, row_id);
        row_new->set_primary_key(key);
        memcpy(row_new->get_data(), row->get_data(), row->get_tuple_size());

        index_insert(i_customer_id, key, row_new, wh_to_part(wid), row_id);
	}
}

void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
    row_t * row;
    uint64_t row_id;
    t_history->get_new_row(row, 0, row_id);
    row->set_primary_key(0);
    row->set_value(H_C_ID, c_id);
    row->set_value(H_C_D_ID, d_id);
    row->set_value(H_D_ID, d_id);
    row->set_value(H_C_W_ID, w_id);
    row->set_value(H_W_ID, w_id);
    row->set_value(H_DATE, 0);
    row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data, w_id-1);
    row->set_value(H_DATA, h_data);
#endif

}

void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
        row_t *row;
        uint64_t row_id;
        t_order->get_new_row(row, 0, row_id);
        row->set_primary_key(orderKey(oid, did, wid));
        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm[oid - 1]; //get_permutation();
        row->set_value(O_ID, oid);
        row->set_value(O_C_ID, cid);
        row->set_value(O_D_ID, did);
        row->set_value(O_W_ID, wid);
        uint64_t o_entry = 2013;
        row->set_value(O_ENTRY_D, o_entry);
        if (oid < 2101)
            row->set_value(O_CARRIER_ID, URand(1, 10, wid - 1));
        else
            row->set_value(O_CARRIER_ID, 0);
        o_ol_cnt = URand(5, 15, wid - 1);
        row->set_value(O_OL_CNT, o_ol_cnt);
        row->set_value(O_ALL_LOCAL, 1);
        // index_insert(i_customer_id, key, row, wh_to_part(wid));

        w_d_cid_oid[wid][did][cid] = oid; //wid->d_id_cid->oid

        index_insert(i_order, orderKey(oid, did, wid), row, wh_to_part(wid), row_id);

//        row_t *row_cust;
//        t_order->get_new_row(row_cust, 0, row_id);
//        row_cust->set_primary_key(orderCustKey(oid, cid, did, wid));
//        memcpy(row_cust->get_data(), row->get_data(), row->get_tuple_size());
//        index_insert(i_order_cust, orderCustKey(oid, cid, did, wid), row_cust,  wh_to_part(wid));
        // ORDER-LINE
#if !TPCC_SMALL
        for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
            t_orderline->get_new_row(row, 0, row_id);
            row->set_primary_key(orderlineKey(ol, oid, did, wid));
            row->set_value(OL_O_ID, oid);
            row->set_value(OL_D_ID, did);
            row->set_value(OL_W_ID, wid);
            row->set_value(OL_NUMBER, ol);
            row->set_value(OL_I_ID, URand(1, 100000, wid - 1));
            row->set_value(OL_SUPPLY_W_ID, wid);
            if (oid < 2101) {
                row->set_value(OL_DELIVERY_D, o_entry);
                row->set_value(OL_AMOUNT, 0);
            } else {
                row->set_value(OL_DELIVERY_D, 0);
                row->set_value(OL_AMOUNT, (double) URand(1, 999999, wid - 1) / 100);
            }
            row->set_value(OL_QUANTITY, 5);
            char ol_dist_info[24];
            MakeAlphaString(24, 24, ol_dist_info, wid - 1);
            row->set_value(OL_DIST_INFO, ol_dist_info);

            index_insert(i_orderline, orderlineKey(ol, oid, did, wid), row, wh_to_part(wid), row_id);
        }
#endif
        // NEW ORDER
        if (oid > 2100) {
            t_neworder->get_new_row(row, 0, row_id);
            row->set_primary_key(neworderKey(oid, did, wid));
            row->set_value(NO_O_ID, (int64_t) oid);
            row->set_value(NO_D_ID, did);
            row->set_value(NO_W_ID, wid);
            index_insert(i_neworder, neworderKey(oid, did, wid), row, wh_to_part(wid), row_id);
        }
    }
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for(i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for(i=0; i < g_cust_per_dist-1; i++) {
        uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
    tpcc_wl * wl = (tpcc_wl *) This;
    int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
    uint32_t wid = tid + 1;
    assert((uint64_t)tid < g_num_wh);

//    if (tid == 0)
//        wl->init_tab_item();
    wl->init_tab_wh( wid );
    wl->init_tab_dist( wid );
    wl->init_tab_stock( wid );
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
        wl->init_tab_cust(did, wid);
        wl->init_tab_order(did, wid);
//        for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
//            wl->init_tab_hist(cid, did, wid);
    }
    return NULL;
}
