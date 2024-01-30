#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "stats.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "btree_store.h"

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;
}

RC tpcc_txn_man::run_txn(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
	switch (m_query->type) {
        case TPCC_QUERY2:
            return run_query2(m_query); break;
		case TPCC_PAYMENT :
			return run_payment(m_query); break;
		case TPCC_NEW_ORDER :
			return run_new_order(m_query); break;
		case TPCC_ORDER_STATUS :
			return run_order_status(m_query); break;
		case TPCC_DELIVERY :
			return run_delivery(m_query); break;
		case TPCC_STOCK_LEVEL :
			return run_stock_level(m_query); break;
		default:
			assert(false);
	}
}

//////////////////////////////////////////////////////
// Payment
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::payment_getWarehouse(uint64_t w_id) {
    // SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?
    auto index = _wl->i_warehouse;
    auto key = warehouseKey(w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, g_wh_update ? WR : RD);
}

void tpcc_txn_man::payment_updateWarehouseBalance(row_t* row, double h_amount) {
    // UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?
    double w_ytd;
    row->get_value(W_YTD, w_ytd);
    if (g_wh_update) row->set_value(W_YTD, w_ytd + h_amount);
}

row_t* tpcc_txn_man::payment_getDistrict(uint64_t d_w_id, uint64_t d_id) {
    // SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
    auto index = _wl->i_district;
    auto key = distKey(d_id, d_w_id);
    auto part_id = wh_to_part(d_w_id);
    return search(index, key, part_id, WR);
}

void tpcc_txn_man::payment_updateDistrictBalance(row_t* row, double h_amount) {
    // UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?
    double d_ytd;
    row->get_value(D_YTD, d_ytd);
    row->set_value(D_YTD, d_ytd + h_amount);
}

row_t* tpcc_txn_man::payment_getCustomerByCustomerId(uint64_t w_id,
                                                     uint64_t d_id,
                                                     uint64_t c_id) {
    // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    auto index = _wl->i_customer_id;
    auto key = custKey(c_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, WR);
}

//not support now
row_t* tpcc_txn_man::payment_getCustomerByLastName(uint64_t w_id, uint64_t d_id,
                                                   const char* c_last,
                                                   uint64_t* out_c_id) {
    // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST;
    // XXX: the list is not sorted. But let's assume it's sorted...
    // The performance won't be much different.
//    auto index = _wl->i_customer_last;
//    auto key = custNPKey(d_id, w_id, c_last);
//    auto part_id = wh_to_part(w_id);
//
//    itemid_t* items[100];
//    size_t count = 100;
//    RC rc;
////    rc = index_read_multiple(index, key, items, count, part_id);
//    if (rc != RCOK) {
//        assert(false);
//        return NULL;
//    }
//    if (count == 0) return NULL;
//    // assert(count != 100);
//
//    auto mid = items[count / 2];
//    auto local = get_row((row_t *)mid->location, WR);
//
//    if (local != NULL) local->get_value(C_ID, *out_c_id);
//
//    // printf("payment_getCustomerByLastName: %" PRIu64 "\n", cnt);
//    return local;
    return nullptr;
}

bool tpcc_txn_man::payment_updateCustomer(row_t* row, uint64_t c_id,
                                          uint64_t c_d_id, uint64_t c_w_id,
                                          uint64_t d_id, uint64_t w_id,
                                          double h_amount) {
    // UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    // UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    double c_balance;
    row->get_value(C_BALANCE, c_balance);
    row->set_value(C_BALANCE, c_balance - h_amount);
    double c_ytd_payment;
    row->get_value(C_YTD_PAYMENT, c_ytd_payment);
    row->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
    uint64_t c_payment_cnt;
    row->get_value(C_PAYMENT_CNT, c_payment_cnt);
    row->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

    const char* c_credit = row->get_value(C_CREDIT);
    if (strstr(c_credit, "BC")) {
        char c_new_data[501];
        sprintf(c_new_data, "%4d %2d %4d %2d %4d $%7.2f | %c", (int)c_id, (int)c_d_id,
                (int)c_w_id, (int)d_id, (int)w_id, h_amount, '\0');

        const char* c_data = row->get_value(C_DATA);
        strncat(c_new_data, c_data, 500 - strlen(c_new_data));
        row->set_value(C_DATA, c_new_data);
    }
    return true;
}

bool tpcc_txn_man::payment_insertHistory(uint64_t c_id, uint64_t c_d_id,
                                         uint64_t c_w_id, uint64_t d_id,
                                         uint64_t w_id, uint64_t h_date,
                                         double h_amount, const char* h_data) {
// INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)
//    row_t* row = NULL;
//    uint64_t row_id;
//    auto part_id = wh_to_part(w_id);
//    if (!insert_row_to_table(row, _wl->t_history, part_id, row_id)) return false;
//
//    row->set_primary_key(0);
//    row->set_value(H_C_ID, c_id);
//    row->set_value(H_C_D_ID, c_d_id);
//    row->set_value(H_C_W_ID, c_w_id);
//    row->set_value(H_D_ID, d_id);
//    row->set_value(H_W_ID, w_id);
//    row->set_value(H_DATE, h_date);
//    row->set_value(H_AMOUNT, h_amount);
//    row->set_value(H_DATA, const_cast<char*>(h_data));

    // No index to update.
    return true;
}

//////////////////////////////////////////////////////
// New Order
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::new_order_getWarehouseTaxRate(uint64_t w_id) {
    // SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?
    auto index = _wl->i_warehouse;
    auto key = warehouseKey(w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::new_order_getDistrict(uint64_t d_id, uint64_t d_w_id) {
    // SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?
    auto index = _wl->i_district;
    auto key = distKey(d_id, d_w_id);
    auto part_id = wh_to_part(d_w_id);
    // if (d_id == 1)
    //   lock_print = true;
    return search(index, key, part_id, WR);
}

void tpcc_txn_man::new_order_incrementNextOrderId(row_t* row,
                                                  int64_t* out_o_id) {
    // UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?
    int64_t o_id;
    row->get_value(D_NEXT_O_ID, o_id);
    // printf("%" PRIi64 "\n", o_id);
    *out_o_id = o_id;
    o_id++;
    row->set_value(D_NEXT_O_ID, o_id);
}

row_t* tpcc_txn_man::new_order_getCustomer(uint64_t w_id, uint64_t d_id,
                                           uint64_t c_id) {
    // SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    auto index = _wl->i_customer_id;
    auto key = custKey(c_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, RD);
}

bool tpcc_txn_man::new_order_createOrder(int64_t o_id, uint64_t d_id,
                                         uint64_t w_id, uint64_t c_id,
                                         uint64_t o_entry_d,
                                         uint64_t o_carrier_id, uint64_t ol_cnt,
                                         bool all_local) {
// INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    row_t *row = NULL, *row_cust = NULL;
    uint64_t row_id;
    auto part_id = wh_to_part(w_id);
    if (!insert_row_to_table(row, _wl->t_order, part_id, row_id)) return false;

    if (!insert_row_to_table(row_cust, _wl->t_order, part_id, row_id)) return false;

    row->set_primary_key(orderKey(o_id, d_id, w_id));
    row->set_value(O_ID, o_id);
    row->set_value(O_D_ID, d_id);
    row->set_value(O_W_ID, w_id);
    row->set_value(O_C_ID, c_id);
    row->set_value(O_ENTRY_D, o_entry_d);
    row->set_value(O_CARRIER_ID, o_carrier_id);
    row->set_value(O_OL_CNT, ol_cnt);
    row->set_value(O_ALL_LOCAL, all_local ? uint64_t(1) : uint64_t(0));

    row_cust->set_primary_key(orderCustKey(o_id, c_id, d_id, w_id));
    memcpy(row_cust->get_data(), row->get_data(), row->get_tuple_size());

 #if TPCC_INSERT_INDEX
    // printf("new Order o_id=%" PRId64 "\n", o_id);
    {
        auto idx = _wl->i_order;
        auto key = orderKey(o_id, d_id, w_id);
        if (!insert_row_to_index(idx, key, row, part_id)) {
            return false;
        }
    }
    {
        auto idx = _wl->i_order_cust;
        auto key = orderCustKey(o_id, c_id, d_id, w_id);
        if (!insert_row_to_index(idx, key, row_cust, part_id)) {
            return false;
        }
    }
 #endif
    return true;
}

bool tpcc_txn_man::new_order_createNewOrder(int64_t o_id, uint64_t d_id,
                                            uint64_t w_id) {
// INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)
    row_t* row = NULL;
    uint64_t row_id;
    auto part_id = wh_to_part(w_id);
    if (!insert_row_to_table(row, _wl->t_neworder, part_id, row_id)) return false;
    row->set_primary_key(neworderKey(o_id, d_id, w_id));
    row->set_value(NO_O_ID, o_id);
    row->set_value(NO_D_ID, d_id);
    row->set_value(NO_W_ID, w_id);

 #if TPCC_INSERT_INDEX
    // printf("new NewOrder o_id=%" PRId64 "\n", o_id);
    {
        auto idx = _wl->i_neworder;
        auto key = neworderKey(o_id, d_id, w_id);
        if (!insert_row_to_index(idx, key, row, part_id)) return false;
    }
 #endif
    return true;
}

row_t* tpcc_txn_man::new_order_getItemInfo(uint64_t ol_i_id) {
    // SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?
    auto index = _wl->i_item;
    auto key = itemKey(ol_i_id);
    auto part_id = 0;
    return search(index, key, part_id, RD);
}

row_t* tpcc_txn_man::new_order_getStockInfo(uint64_t ol_i_id,
                                            uint64_t ol_supply_w_id) {
    // SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?
    auto index = _wl->i_stock;
    auto key = stockKey(ol_i_id, ol_supply_w_id);
    auto part_id = wh_to_part(ol_supply_w_id);
    return search(index, key, part_id, WR);
}

void tpcc_txn_man::new_order_updateStock(row_t* row, uint64_t ol_quantity,
                                         bool remote) {
    // UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?
    uint64_t s_quantity;
    row->get_value(S_QUANTITY, s_quantity);
    uint64_t s_ytd;
    uint64_t s_order_cnt;
    row->get_value(S_YTD, s_ytd);
    row->set_value(S_YTD, s_ytd + ol_quantity);
    row->get_value(S_ORDER_CNT, s_order_cnt);
    row->set_value(S_ORDER_CNT, s_order_cnt + 1);
    if (remote) {
        uint64_t s_remote_cnt;
        row->get_value(S_REMOTE_CNT, s_remote_cnt);
        row->set_value(S_REMOTE_CNT, s_remote_cnt + 1);
    }
    uint64_t quantity;
    if (s_quantity > ol_quantity + 10)
        quantity = s_quantity - ol_quantity;
    else
        quantity = s_quantity - ol_quantity + 91;
    row->set_value(S_QUANTITY, quantity);
}

bool tpcc_txn_man::new_order_createOrderLine(
        int64_t o_id, uint64_t d_id, uint64_t w_id, uint64_t ol_number,
        uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_delivery_d,
        uint64_t ol_quantity, double ol_amount, const char* ol_dist_info) {
// INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    row_t* row = NULL;
    uint64_t row_id;
    auto part_id = wh_to_part(w_id);
    if (!insert_row_to_table(row, _wl->t_orderline, part_id, row_id)) {
        // printf("insert_row.\n");
        return false;
    }
    row->set_primary_key(orderlineKey(ol_number, o_id, d_id, w_id));
    row->set_value(OL_O_ID, o_id);
    row->set_value(OL_D_ID, d_id);
    row->set_value(OL_W_ID, w_id);
    row->set_value(OL_NUMBER, ol_number);
    row->set_value(OL_I_ID, ol_i_id);
    row->set_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    row->set_value(OL_DELIVERY_D, ol_delivery_d);
    row->set_value(OL_QUANTITY, ol_quantity);
    row->set_value(OL_AMOUNT, ol_amount);
    row->set_value(OL_DIST_INFO, const_cast<char*>(ol_dist_info));

 #if TPCC_INSERT_INDEX
    {
        auto idx = _wl->i_orderline;
        auto key = orderlineKey(ol_number, o_id, d_id, w_id);
        if (!insert_row_to_index(idx, key, row, part_id)) {
            // printf("ol_i_id=%d o_id=%d d_id=%d w_id=%d\n", (int)ol_i_id, (int)o_id,
            //        (int)d_id, (int)w_id);
            return false;
        }
    }
 #endif
    return true;
}

//////////////////////////////////////////////////////
// Order Status
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::order_status_getCustomerByCustomerId(uint64_t w_id,
                                                          uint64_t d_id,
                                                          uint64_t c_id) {
    // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?
    auto index = _wl->i_customer_id;
    auto key = custKey(c_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, RD);
}

//now not supported
row_t* tpcc_txn_man::order_status_getCustomerByLastName(uint64_t w_id,
                                                        uint64_t d_id,
                                                        const char* c_last,
                                                        uint64_t* out_c_id) {
    // SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST
    // XXX: the list is not sorted. But let's assume it's sorted...
    // The performance won't be much different.
//    auto index = _wl->i_customer_last;
//    auto key = custNPKey(d_id, w_id, c_last);
//    auto part_id = wh_to_part(w_id);
//
//    itemid_t* items[100];
//    size_t count = 100;
//    RC rc;
////    rc = index_read_multiple(index, key, items, count, part_id);
//    if (rc != RCOK) {
//        assert(false);
//        return NULL;
//    }
//    if (count == 0) return NULL;
//    // assert(count != 100);
//
//    auto mid = items[count / 2];
//    auto local = get_row((row_t *)mid->location, RD);
//    if (local != NULL) local->get_value(C_ID, *out_c_id);
//    // printf("order_status_getCustomerByLastName: %" PRIu64 "\n", cnt);
//    return local;
}

row_t* tpcc_txn_man::order_status_getLastOrder(uint64_t w_id, uint64_t d_id,
                                               uint64_t c_id) {
    // SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1
//    auto index = _wl->i_order_cust;
//    auto key = orderCustKey(g_max_orderline, c_id, d_id, w_id);
//    auto max_key = orderCustKey(1, c_id, d_id, w_id);
//    auto part_id = wh_to_part(w_id);
//
//    itemid_t* items[1];
//    uint64_t count = 1;
//
//    auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
//    if (idx_rc == Abort) return NULL;
//    assert(idx_rc == RCOK);
//
//    if (count == 0) {
//        // There must be at least one order per customer.
//        // printf("order_status_getLastOrder: w_id=%PRIu64 d_id=%PRIu64 c_id=%PRIu64 count=%PRIu64\n",
//        //        w_id, d_id, c_id, count);
//        assert(false);
//        return NULL;
//    }
////   if (count == 0) return NULL;
//
//    auto shared = items[0];
//    auto local = get_row((row_t *)shared->location, RD);
//    if (local == NULL) return NULL;
//    return local;
}
uint64_t tpcc_txn_man::order_status_getOrderId( uint64_t w_id, uint64_t d_id, uint64_t c_id) {
    uint64_t c_o_id =  _wl->w_d_cid_oid[w_id][d_id][c_id];

    return c_o_id;
}
bool tpcc_txn_man::order_status_getOrderLines(uint64_t w_id, uint64_t d_id,
                                              int64_t o_id) {
    // SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?
    auto index = _wl->i_orderline;
    auto key = orderlineKey(1, o_id, d_id, w_id);
    auto max_key = orderlineKey(15, o_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);

//    itemid_t* items[16];
//    std::vector<row_t *> *items = nullptr;
    uint64_t range = 16;
//    int count = 0;
    void *vd_row = NULL;

    auto scan_iter = index->RangeScanBySize(reinterpret_cast<char *>(&key), sizeof(idx_key_t), range);
    auto get_next_row_ = scan_iter ->GetNext();
    uint64_t iteration = 0;
    while (true){
        iteration++;
        if (get_next_row_ == nullptr || iteration > range) {
            break;
        }else{
            vd_row = get_next_row_;
            row_t *orderline;
#if  ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            auto row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row == nullptr || row->is_valid != VALID || row->data == nullptr){
                continue;
            }
            uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
            auto m_item = reinterpret_cast<itemid_t *>(payload);
            if (m_item == NULL) return false;
            auto master_row = m_item->location;
            if (master_row == NULL) return false;
            orderline = get_row(master_row, RO);
#else
            orderline = get_row(vd_row, RO);
#endif
            if (orderline == NULL) return false;

            int64_t ol_i_id;
            uint64_t ol_supply_w_id, ol_quantity, ol_delivery_d;
            double ol_amount;
            orderline->get_value(OL_I_ID, ol_i_id);
            orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
            orderline->get_value(OL_QUANTITY, ol_quantity);
            orderline->get_value(OL_AMOUNT, ol_amount);
            orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
            (void)orderline;
        }
    }

//    count = index_read_range(index, key, max_key, &items, range, part_id);
//    if (count == 0 ) return false;
//    if (items == nullptr) return false;
//    uint64_t items_sz = items->size();
//    if ( count != items_sz) return false;
//    assert(count <= range);
//    printf("count:%d, items_sz:%lu, \n",count,items_sz);
//    for (uint64_t i = 0; i < items_sz; i++) {
//        auto shared = items->at(i);
//        row_t *orderline;
//#if  ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
//        auto row = shared;// row_t meta
//        if (row == nullptr || row->is_valid != VALID || row->data == nullptr){
//            continue;
//        }
//        uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
//        auto m_item = reinterpret_cast<itemid_t *>(payload);
//        if (m_item == NULL) return false;
//        auto master_row = m_item->location;
//        orderline = get_row(master_row, RD);
//#else
//        orderline = get_row(shared, RD);
//#endif
//        if (orderline == NULL) return false;
//
//         int64_t ol_i_id;
//         uint64_t ol_supply_w_id, ol_quantity, ol_delivery_d;
//         double ol_amount;
//        orderline->get_value(OL_I_ID, ol_i_id);
//        orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
//        orderline->get_value(OL_QUANTITY, ol_quantity);
//        orderline->get_value(OL_AMOUNT, ol_amount);
//        orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
//        (void)orderline;
//    }

    // printf("order_status_getOrderLines: w_id=%" PRIu64 " d_id=%" PRIu64
    //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
    //        w_id, d_id, o_id, cnt);
    return true;
}


//////////////////////////////////////////////////////
// Delivery
//////////////////////////////////////////////////////
//
//inline bool tpcc_txn_man::delivery_getNewOrder_deleteNewOrder(uint64_t d_id,
//                                                              uint64_t w_id,
//                                                              int64_t &out_o_id) {
//    // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
//    // DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?
//
//    auto index = _wl->i_neworder;
//    auto key = neworderKey(g_max_orderline, d_id, w_id);
//    auto max_key = neworderKey(0, d_id, w_id);  // Use key ">= 0" for "> -1"
//    auto part_id = wh_to_part(w_id);
//
//    itemid_t* items[1];
//    uint64_t count = 1;
//
////    auto idx_rc = index_read_range_rev(index, key, max_key, items, count, part_id);
////    if (idx_rc == Abort) return false;
////    assert(idx_rc == RCOK);
//
//    // printf("delivery_getNewOrder_deleteNewOrder: %" PRIu64 "\n", count);
//    if (count == 0) {
//        // No new order; this is acceptable and we do not need to abort TX.
//        out_o_id = -1;
//        return true;
//    }
//
//    auto shared = items[0];
//    auto local = get_row((row_t *)shared->location, WR);
//    if (local == NULL) return false;
//
//    int64_t o_id;
//    local->get_value(NO_O_ID, o_id);
//    out_o_id = o_id;
//
//    {
//        auto idx = _wl->i_neworder;
//        auto key = neworderKey(o_id, d_id, w_id);
////        if (!remove_idx(idx, key, (row_t *)items[0]->location, part_id)) return false;
////
////        if (!remove_row((row_t *)shared->location)) return false;
//    }
//    return true;
//}

row_t* tpcc_txn_man::delivery_getCId(int64_t no_o_id, uint64_t d_id,
                                     uint64_t w_id) {
    // SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
    auto index = _wl->i_order;
    auto key = orderKey(no_o_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);
    return search(index, key, part_id, WR);
}

void tpcc_txn_man::delivery_updateOrders(row_t* row, uint64_t o_carrier_id) {
    // UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
    row->set_value(O_CARRIER_ID, o_carrier_id);
}

bool tpcc_txn_man::delivery_updateOrderLine_sumOLAmount(uint64_t o_entry_d,
                                                        int64_t no_o_id,
                                                        uint64_t d_id,
                                                        uint64_t w_id,
                                                        double* out_ol_total) {
    // UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?
    // SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
    double ol_total = 0.0;

    auto index = _wl->i_orderline;
    auto key = orderlineKey(1, no_o_id, d_id, w_id);
    auto max_key = orderlineKey(15, no_o_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);

    itemid_t* items[16];
    uint64_t count = 16;

//    auto idx_rc = index_read_range(index, key, max_key, items, count, part_id);
//    if (idx_rc != RCOK) return false;
//    assert(count != 16);

    for (uint64_t i = 0; i < count; i++) {
        auto shared = items[i];
        auto local = get_row((row_t *)shared->location, WR);
        if (local == NULL) return false;
        double ol_amount;
        local->get_value(OL_AMOUNT, ol_amount);
        local->set_value(OL_DELIVERY_D, o_entry_d);
        ol_total += ol_amount;
    }

    // printf("delivery_updateOrderLine_sumOLAmount: w_id=%" PRIu64 " d_id=%" PRIu64
    //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
    //        w_id, d_id, no_o_id, cnt);
    *out_ol_total = ol_total;
    return true;
}

bool tpcc_txn_man::delivery_updateCustomer(double ol_total, uint64_t c_id,
                                           uint64_t d_id, uint64_t w_id) {
    // UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?
    auto index = _wl->i_customer_id;
    auto key = custKey(c_id, d_id, w_id);
    auto part_id = wh_to_part(w_id);
    auto row = search(index, key, part_id, WR);
    if (row == NULL) return false;

    double c_balance;
    uint64_t c_delivery_cnt;
    row->get_value(C_BALANCE, c_balance);
    row->set_value(C_BALANCE, c_balance + ol_total);
    row->get_value(C_DELIVERY_CNT, c_delivery_cnt);
    row->set_value(C_DELIVERY_CNT, c_delivery_cnt + 1);
    return true;
}

bool tpcc_txn_man::delivery_getNewOrder_deleteNewOrder(uint64_t n_o_id, uint64_t d_id,
                                                       uint64_t w_id, int64_t* out_o_id) {
    // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
    // DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?

    auto index = _wl->i_neworder;
    auto key = neworderKey(g_max_orderline, d_id, w_id);
#ifndef TPCC_SILO_REF_LAST_NO_O_IDS
//  auto max_key = neworderKey(0, d_id, w_id);  // Use key ">= 0" for "> -1"
    auto max_key = neworderKey(n_o_id, d_id, w_id);
#else
    // Use reference Silo's last seen o_id history.
  auto max_key = neworderKey(
      last_no_o_ids[(w_id - 1) * DIST_PER_WARE + d_id - 1].o_id, d_id, w_id);
#endif
    auto part_id = wh_to_part(w_id);

    void* rows[1];
    uint64_t count = 1;

    auto idx_rc = RCOK;
//  index_read_range_rev(index, key, max_key, rows, count, part_id);
    if (idx_rc == Abort) return false;
    assert(idx_rc == RCOK);

    // printf("delivery_getNewOrder_deleteNewOrder: %" PRIu64 "\n", count);
    if (count == 0) {
        // No new order; this is acceptable and we do not need to abort TX.
        *out_o_id = -1;
        return true;
    }

    auto shared = rows[0];
    itemid_t *item;
    auto local = search(index, max_key, part_id, RD);
    //auto local = get_row(index, reinterpret_cast<row_t *>(shared), item, part_id, WR);
    if (local == NULL) return false;

    int64_t o_id;
    local->get_value(NO_O_ID, o_id);
    *out_o_id = o_id;

#ifdef TPCC_SILO_REF_LAST_NO_O_IDS
    last_no_o_ids[(w_id - 1) * DIST_PER_WARE + d_id - 1].o_id = o_id + 1;
#endif

#if TPCC_DELETE_INDEX
    {
    auto idx = _wl->i_neworder;
    auto key = neworderKey(o_id, d_id, w_id);
// printf("o_id=%" PRIi64 " d_id=%" PRIu64 " w_id=%" PRIu64 "\n", o_id, d_id,
//        w_id);
// printf("requesting remove_idx idx=%p key=%" PRIu64 " row_id=%" PRIu64 " part_id=%" PRIu64 " \n",
//        idx, key, (uint64_t)rows[0], part_id);
#if CC_ALG != MICA
    if (!remove_idx(idx, key, rows[0], part_id)) return false;
#else
    if (!remove_idx(idx, key, row, part_id)) return false;
#endif
  }
#endif

#if TPCC_DELETE_ROWS
    #if CC_ALG != MICA
  if (!remove_row(shared)) return false;
#else
  // printf("remove NewOrder o_id=%" PRId64 "\n", o_id);
  // MICA handles row deletion directly without using remove_row().
  if (!rah.write_row(0) || !rah.delete_row()) return false;
#endif
#endif

    return true;
}
//TODO concurrency for index related operations is not completely supported yet.
// In correct states may happen with the current code.

//////////////////////////////////////////////////////
// Stock Level
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::stock_level_getOId(uint64_t d_w_id, uint64_t d_id) {
    // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
    auto index = _wl->i_district;
    auto key = distKey(d_id, d_w_id);
    auto part_id = wh_to_part(d_w_id);
    return search(index, key, part_id, RD);
}

bool tpcc_txn_man::stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
                                             int64_t ol_o_id, uint64_t s_w_id,
                                             uint64_t threshold,
                                             uint64_t* out_distinct_count) {
    /** SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
         WHERE OL_W_ID = ?
           AND OL_D_ID = ?
           AND OL_O_ID < ?
           AND OL_O_ID >= ?
           AND S_W_ID = ?
           AND S_I_ID = OL_I_ID
           AND S_QUANTITY < ?

         20 orders * 15 items = 300; use 301 to check any errors.
     **/

    uint64_t n_orders = 10; //20 number orders
//    uint64_t range = n_orders * 15 + 1; // 20*15+1 number order_lines
    uint64_t range = n_orders * 6 + 1; // 20*15+1 number order_lines

    uint64_t ol_i_id_list[range];
    size_t list_size = 0;
    auto index = _wl->i_orderline;
    auto key = orderlineKey(1, ol_o_id - 1, ol_d_id, ol_w_id);
//    auto max_key = orderlineKey(15, ol_o_id - n_orders, ol_d_id, ol_w_id);
    auto max_key = orderlineKey(6, ol_o_id - n_orders, ol_d_id, ol_w_id);
    auto part_id = wh_to_part(ol_w_id);

//    void* items[count];
//     uint64_t count = 301;
//    std::vector<row_t *> *items = nullptr;
//    auto count = index_read_range(index, key, max_key, &items, range, part_id);
//    if (count == 0) return false;
//    assert(count <= range);
//    int items_sz = items->size();
    void *vd_row = NULL;
    auto scan_iter = index->RangeScanBySize(reinterpret_cast<char *>(&key), sizeof(idx_key_t), range);
    auto get_next_row_ = scan_iter ->GetNext();
    uint64_t iterations = 0;
    while (true) {
        iterations ++;
        if (get_next_row_ == nullptr || iterations > range) {
            break;
        }else{
            vd_row = get_next_row_;
            row_t *orderline;
#if  ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
            auto row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row == nullptr || row->is_valid != VALID || row->data == nullptr){
                continue;
            }
            uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
            auto m_item = reinterpret_cast<itemid_t *>(payload);
            auto master_row = m_item->location;
            orderline = get_row(master_row, RO);
#else
            orderline = get_row(vd_row, RO);
#endif
            if (orderline == NULL) return false;

            uint64_t ol_i_id, ol_supply_w_id;
            orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
            if (ol_supply_w_id != s_w_id) continue;

            orderline->get_value(OL_I_ID, ol_i_id);

            assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
            ol_i_id_list[list_size] = ol_i_id;
            list_size++;
        }
    }

//    for (int i = 0; i < items_sz; i++) {
//        auto orderline_shared = items->at(i);
//        row_t *orderline;
//#if  ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
//        auto row = orderline_shared;// row_t meta
//        if (row == nullptr || row->is_valid != VALID || row->data == nullptr){
//            continue;
//        }
//        uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
//        auto m_item = reinterpret_cast<itemid_t *>(payload);
//        auto master_row = m_item->location;
//        orderline = get_row(master_row, RO);
//#else
//        orderline = get_row(orderline_shared, RD);
//#endif
//        if (orderline == NULL) return false;
//
//        uint64_t ol_i_id, ol_supply_w_id;
//        orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
//        if (ol_supply_w_id != s_w_id) continue;
//
//        orderline->get_value(OL_I_ID, ol_i_id);
//
//        assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
//        ol_i_id_list[list_size] = ol_i_id;
//        list_size++;
//    }
//    items->clear();
//    assert(list_size <= count);

    uint64_t distinct_ol_i_id_list[list_size];
    uint64_t distinct_ol_i_id_count = 0;
    uint64_t result = 0;

    for (uint64_t i = 0; i < list_size; i++) {
        uint64_t ol_i_id = ol_i_id_list[i];

        bool duplicate = false;
        for (uint64_t j = 0; j < distinct_ol_i_id_count; j++)
            if (distinct_ol_i_id_list[j] == ol_i_id) {
                duplicate = true;
                break;
            }
        if (duplicate) continue;

        distinct_ol_i_id_list[distinct_ol_i_id_count++] = ol_i_id;

        auto key = stockKey(ol_i_id, s_w_id);
        auto index = _wl->i_stock;
        auto part_id = wh_to_part(s_w_id);
        auto row = search(index, key, part_id, RO);
        if (row == NULL) return false;

        uint64_t s_quantity;
        row->get_value(S_QUANTITY, s_quantity);
        if (s_quantity < threshold) result++;
    }

    // printf("stock_level_getStockCount: w_id=%" PRIu64 " d_id=%" PRIu64
    //        " o_id=%" PRIu64 " s_w_id=%" PRIu64 " list_size=%" PRIu64
    //        " distinct_cnt=%" PRIu64 " result=%" PRIu64 "\n",
    //        ol_w_id, ol_d_id, ol_o_id, s_w_id, list_size, distinct_ol_i_id_count,
    //        result);
    *out_distinct_count = result;
    return true;
}

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
    row_t* items[15];
    assert(query->ol_cnt <= sizeof(items) / sizeof(items[0]));
    for (uint64_t ol_number = 1; ol_number <= query->ol_cnt; ol_number++) {
        items[ol_number - 1] =
                new_order_getItemInfo(query->items[ol_number - 1].ol_i_id);
        // printf("ol_i_id %d\n", (int)arg.items[ol_number - 1].ol_i_id);
        if (items[ol_number - 1] == NULL) {
            // FAIL_ON_ABORT();
            return finish(Abort);
        };
    }

    auto warehouse = new_order_getWarehouseTaxRate(query->w_id);
    if (warehouse == NULL) {
        return finish(Abort);
    };
    double w_tax;
    warehouse->get_value(W_TAX, w_tax);

    // Read-modify-write.
    auto district = new_order_getDistrict(query->d_id, query->w_id);
    if (district == NULL) {
        return finish(Abort);
    };

    // usleep(1000);
    double d_tax;
    district->get_value(D_TAX, d_tax);
    int64_t d_o_n_id;
    district->get_value(D_NEXT_O_ID, d_o_n_id);

    int64_t o_id = 0;// = __sync_fetch_and_add(&(o_ids[arg.w_id][arg.d_id]), 1);
    new_order_incrementNextOrderId(district, &o_id);

//    printf("d_o_n_id: %lu \n", o_id);

    auto customer = new_order_getCustomer(query->w_id, query->d_id, query->c_id);
    if (customer == NULL) {
        return finish(Abort);
    };
     double c_discount;
     customer->get_value(C_DISCOUNT, c_discount);
//     printf("c_discount: %lu \n", c_discount);

 #if TPCC_INSERT_ROWS
    uint64_t o_carrier_id = 0;
    if (!new_order_createOrder(o_id, query->d_id, query->w_id, query->c_id, query->o_entry_d,
                               o_carrier_id, query->ol_cnt, query->all_local)) {
        return finish(Abort);
    };

    if (!new_order_createNewOrder(o_id, query->d_id, query->w_id)) {
        // printf("new_order_createNewOrder fail.\n");
        return finish(Abort);
    };
 #endif

    for (uint64_t ol_number = 1; ol_number <= query->ol_cnt; ol_number++) {
        uint64_t ol_i_id = query->items[ol_number - 1].ol_i_id;
        uint64_t ol_supply_w_id = query->items[ol_number - 1].ol_supply_w_id;
        uint64_t ol_quantity = query->items[ol_number - 1].ol_quantity;

        auto stock = new_order_getStockInfo(ol_i_id, ol_supply_w_id);
        if (stock == NULL) {
            // printf("%lld, %lld\n", ol_i_id, ol_supply_w_id);
            return finish(Abort);
        };
        bool remote = ol_supply_w_id != query->w_id;
        new_order_updateStock(stock, ol_quantity, remote);


 #if TPCC_INSERT_ROWS
        double i_price;
        items[ol_number - 1]->get_value(I_PRICE, i_price);
        double ol_amount = ol_quantity * i_price;
        assert(query->d_id >= 1 && query->d_id <= DIST_PER_WARE);
        const char* ol_dist_info = stock->get_value(S_DIST_01 + query->d_id - 1);
        if (!new_order_createOrderLine(o_id, query->d_id, query->w_id, ol_number, ol_i_id,
                                       ol_supply_w_id, query->o_entry_d, ol_quantity,
                                       ol_amount, ol_dist_info)) {
            return finish(Abort);
        };
 #endif
    }
    return finish(RCOK);
}

RC tpcc_txn_man::run_payment(tpcc_query * query) {

    //1.search warehouse
    //  update warehouse
    auto warehouse = payment_getWarehouse(query->w_id);
    if (warehouse == NULL) {
        return finish(Abort);
    }

    // printf("txn=%d, %d-%d\n", get_thd_id(), arg.w_id, arg.d_id);

    payment_updateWarehouseBalance(warehouse, query->h_amount);

    //2.search district
    //  update district
    auto district = payment_getDistrict(query->w_id, query->d_id);
    if (district == NULL) {
        return finish(Abort );
    };
    payment_updateDistrictBalance(district, query->h_amount);

    auto c_id = query->c_id;
    row_t* customer;
    if (!query->by_last_name)
        customer = payment_getCustomerByCustomerId(query->w_id, query->d_id, query->c_id);
    else
        customer = payment_getCustomerByLastName(query->w_id, query->d_id, query->c_last, &c_id);
    if (customer == NULL) {
        return finish(Abort);
    };

    if (!payment_updateCustomer(customer, c_id, query->c_d_id, query->c_w_id, query->d_id,
                                query->w_id, query->h_amount)) {
        return finish(Abort);
    }

#if TPCC_INSERT_ROWS
    // now does not insert row
    char w_name[11];
    char* tmp_str = warehouse->get_value(W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';

    char d_name[11];
    // tmp_str = district->get_value(D_NAME);//
    // memcpy(d_name, tmp_str, 10);//
    d_name[10] = '\0';

    char h_data[25];
    strcpy(h_data, w_name);
    int length = strlen(h_data);
    strcpy(&h_data[length], "    ");
    strcpy(&h_data[length + 4], d_name);
    h_data[length + 14] = '\0';

    if (!payment_insertHistory(c_id, query->c_d_id, query->c_w_id, query->d_id, query->w_id,
                               query->h_date, query->h_amount, h_data)) {
        return finish(Abort);
    };
#endif

    return finish(RCOK);
}

RC tpcc_txn_man::run_delivery(tpcc_query * query) {
//    auto& arg = query;
    int64_t o_id = 0;
    uint64_t n_o_oid = query->n_o_id;
    for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++)
    {
        o_id = 0;
        if (!delivery_getNewOrder_deleteNewOrder(n_o_oid, d_id, query->w_id, &o_id)) {
            return finish(Abort);
        }

        // No new order for this district.
        if (o_id == -1) {
            continue;
        }

        auto order = delivery_getCId(o_id, d_id, query->w_id);
        if (order == NULL) {
            return finish(Abort);
        }
        uint64_t c_id;
        order->get_value(O_C_ID, c_id);

        delivery_updateOrders(order, query->o_carrier_id);

        double ol_total;
#ifdef TPCC_CAVALIA_NO_OL_UPDATE
        if (!delivery_updateOrderLine_sumOLAmount(query->ol_delivery_d, o_id, d_id,
                                                  query->w_id, &ol_total)) {

            // INC_STATS_ALWAYS(get_thd_id(), debug2, 1);
            return finish(Abort);
        }
#else
        ol_total = 1.;
#endif

        if (!delivery_updateCustomer(ol_total, c_id, d_id, query->w_id)) {
            // printf("oops3\n");
            // INC_STATS_ALWAYS(get_thd_id(), debug3, 1);
            return finish(Abort);
        }
    }

    auto rc = finish(RCOK);
// if (rc != RCOK) INC_STATS_ALWAYS(get_thd_id(), debug4, 1);
    return rc;
}
RC tpcc_txn_man::run_order_status(tpcc_query * query) {
#if TPCC_FULL
//    auto& arg = query->args.order_status;

    auto c_id = query->c_id;
    row_t* customer;
    if (!query->by_last_name)
        customer = order_status_getCustomerByCustomerId(query->w_id, query->d_id, query->c_id);
    else
        customer = order_status_getCustomerByLastName(query->w_id, query->d_id,
                                                      query->c_last, &c_id);
    if (customer == NULL) {
        return finish(Abort);
    };

//    auto order = order_status_getLastOrder(query->w_id, query->d_id, c_id);
    auto oid = order_status_getOrderId(query->w_id, query->d_id, c_id);
//    if (order != NULL) {
    int64_t o_id = oid;

    if (!order_status_getOrderLines(query->w_id, query->d_id, o_id)) {
        return finish(Abort);
    };
//    }
#endif

    return finish(RCOK);
}

RC tpcc_txn_man::run_stock_level(tpcc_query * query) {
// #if TPCC_FULL

    auto district = stock_level_getOId(query->w_id, query->d_id);
    if (district == NULL) {
        return finish(Abort);
    }
    int64_t o_id;
    district->get_value(D_NEXT_O_ID, o_id);

    o_id = query->n_o_id;

    uint64_t distinct_count;
    if (!stock_level_getStockCount(query->w_id, query->d_id, o_id, query->w_id,
                                   query->threshold, &distinct_count))
    {
        return finish(Abort);
    }
    (void)distinct_count;
// #endif

    return finish(RCOK);
}


RC tpcc_txn_man::run_query2(tpcc_query * query ) {
    /*  "query_2": {
                   "SELECT su_suppkey, su_name,  n_name, i_id, i_name, su_address, su_phone, "
                    + "su_comment FROM item, supplier, stock, nation, region, "
                    + "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity  FROM stock, "
                    + "supplier, nation, region  WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey "
                    + "AND su_nationkey=n_nationkey AND n_regionkey=r_regionkey "
                    + "AND r_name LIKE 'Europ%'  GROUP BY s_i_id) m "
                    + "WHERE i_id = s_i_id  AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND su_nationkey = n_nationkey  AND n_regionkey = r_regionkey "
                    + "AND i_data LIKE '%b'  AND r_name LIKE 'Europ%' "
                    + "AND i_id=m_i_id  AND s_quantity = m_s_quantity "
                    + "ORDER BY n_name,  su_name,  i_id"
     }  */
    auto regions_ = _wl->ch_regions;
    auto nations_ = _wl->ch_nations;
    auto suppliers_ = _wl->ch_suppliers;
    auto supplier_item_map = _wl->supp_stock_map;// ,w_id,i_id
    uint64_t supplier_num = suppliers_.size();
    // Pick a target region
    auto target_region = GetRandomInteger(0, 4);
    //	auto target_region = 3;
    assert(0 <= target_region and target_region <= 4);
    auto stock_index = _wl->i_stock;

    uint64_t starttime = get_sys_clock();

    // Scan region
    for (auto &r_r : regions_) {
        uint64_t r_id = r_r.second->region_id;
        std::string r_name = r_r.second->region_name;

        // filtering region
        if (r_name != std::string(regions[target_region])) continue;

        // Scan nation
        for (auto &r_n : nations_) {
            uint64_t n_id = r_n.second->nation_id;
            std::string n_name = r_n.second->nation_name;
            // filtering nation
            if (r_r.second->region_id != r_n.second->region_id) continue;
            // Scan suppliers
            for (uint64_t i = 0; i < supplier_num; i++) {
                auto suppll = suppliers_.find(i);
                if (r_n.second->nation_id != suppll->second->su_nation_id) continue;

                int64_t min_w_id = 0;
                int64_t min_i_id = 0;
                int64_t min_s_quant = 0;
                int64_t min_s_ytd = 0;
                int64_t min_s_ord_cnt = 0;
                int64_t min_s_rem_cnt = 0;

                // aggregate - finding a stock tuple having min. stock level
                int16_t min_qty = std::numeric_limits<int16_t>::max();
                // "mod((s_w_id*s_i_id),10000)=su_suppkey"
                // items
                int64_t supp_id = suppll->second->su_supp_id;
                for (auto &it : supplier_item_map[supp_id])  // already know
                {
                    uint64_t stock_k = stockKey(it.second, it.first);
                    auto part_id = wh_to_part(it.second);
                    auto stock_row = search(stock_index, stock_k, part_id, RO);
                    if (stock_row == NULL)  continue;

                    int64_t s_w_id, s_i_id ,s_quantity ,s_ytd, s_order_cnt, s_remote_cnt ;
                    stock_row->get_value(S_W_ID, s_w_id);
                    stock_row->get_value(S_I_ID, s_i_id);
                    stock_row->get_value(S_QUANTITY, s_quantity);
                    stock_row->get_value(S_YTD, s_ytd);
                    stock_row->get_value(S_ORDER_CNT, s_order_cnt);
                    stock_row->get_value(S_REMOTE_CNT, s_remote_cnt);
                    //ASSERT(s_w_id * s_i_id % 10000 == supp_id);
                    if (min_qty > s_quantity) {
                        min_w_id = s_w_id;
                        min_i_id = s_i_id;
                        min_s_quant = s_quantity;
                        min_s_ytd = s_ytd;
                        min_s_ord_cnt = s_order_cnt;
                        min_s_rem_cnt = s_remote_cnt;
                    }
                }

                // fetch the (lowest stock level) item info
                auto index = _wl->i_item;
                auto key = itemKey(min_i_id);
                auto part_id = 0;
                auto item_row = search(index, key, part_id, RO);

                if (item_row == NULL) continue;
                char *item_data = item_row->data;
                char *i_data = item_data + 48;
                std::string str_i_data = std::string(i_data, 50);
                //  filtering item (i_data like '%b')
                auto found = str_i_data.find('b');
                if (found != std::string::npos) continue;

                // XXX. read-mostly txn: update stock or item here

                if (min_s_quant < 15) {
                    uint64_t stock_k = stockKey(min_i_id, min_w_id); //item_id, warehouse_id
                    auto part_id = wh_to_part(min_w_id);
                    auto stock_row = search(stock_index, stock_k, part_id, WR);
                    if (stock_row == NULL) return finish(Abort);

                    stock_row->set_value(S_QUANTITY, min_s_quant+50);
                    stock_row->set_value(S_YTD, min_s_ytd);
                    stock_row->set_value(S_ORDER_CNT, min_s_ord_cnt);
                    stock_row->set_value(S_REMOTE_CNT, min_s_rem_cnt);
                }

                /*  cout << k_su.su_suppkey              << ","
                         << v_su->su_name                << ","
                         << v_n->n_name                  << ","
                         << k_i.i_id                     << ","
                         << v_i->i_name                  << std::endl;  */
            }
        }
    }

    uint64_t timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), time_q2, timespan);

    return finish(RCOK);
}