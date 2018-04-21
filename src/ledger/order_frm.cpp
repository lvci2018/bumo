/*
bumo is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

bumo is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with bumo.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "order_frm.h"
#include <utils/logger.h>
#include "ledger_frm.h"
using namespace soci;

namespace bumo {

	const char* OrderFrame::kSQLCreateStatement1 =
		"CREATE TABLE orders"
		"("
		"sellerid         VARCHAR(56)  NOT NULL,"
		"orderid          VARCHAR(128) NOT NULL CHECK (orderid >= 0),"
		"sellingassettype INT          NOT NULL,"
		"sellingassetcode VARCHAR(12),"
		"sellingissuer    VARCHAR(56),"
		"buyingassettype  INT          NOT NULL,"
		"buyingassetcode  VARCHAR(12),"
		"buyingissuer     VARCHAR(56),"
		"amount           BIGINT           NOT NULL CHECK (amount >= 0),"
		"pricen           INT              NOT NULL,"
		"priced           INT              NOT NULL,"
		"price            DOUBLE PRECISION NOT NULL,"
		"flags            INT              NOT NULL,"
		"lastmodified     INT              NOT NULL,"
		"txhash           VARCHAR(64)      NOT NULL,"
		"opindex		  INT			   NOT NULL,"
		"PRIMARY KEY      (orderid)"
		");";

	const char* OrderFrame::kSQLCreateStatement2 =
		"CREATE INDEX sellingissuerindex ON orders (sellingissuer);";

	const char* OrderFrame::kSQLCreateStatement3 =
		"CREATE INDEX buyingissuerindex ON orders (buyingissuer);";

	const char* OrderFrame::kSQLCreateStatement4 =
		"CREATE INDEX priceindex ON orders (price);";

	static const char* orderColumnSelector =
		"SELECT sellerid,orderid,sellingassettype,sellingassetcode,sellingissuer,"
		"buyingassettype,buyingassetcode,buyingissuer,amount,pricen,priced,"
		"flags,lastmodified,txhash,opindex "
		"FROM orders";


	OrderFrame::OrderFrame(){}
	OrderFrame::OrderFrame(protocol::Order const& from) {
		order_.CopyFrom(from);
	}
	OrderFrame::OrderFrame(OrderFrame const& from){
		order_.CopyFrom(from.order_);
	}
	OrderFrame& OrderFrame::operator=(OrderFrame const& other){
		if (&other != this){
			order_.CopyFrom(other.order_);
		}
		return *this;
	}

	protocol::Price const& OrderFrame::GetPrice() const{
		return order_.remain_order().price();
	}

	int64_t OrderFrame::GetAmount() const{
		return order_.remain_order().amount();
	}

	void OrderFrame::SetAmount(int64_t amount){
		assert(amount >= 0);
		order_.mutable_remain_order()->set_amount(amount);
	}

	std::string const& OrderFrame::GetSellerID() const{
		return order_.seller_address();
	}

	void OrderFrame::SetSellerID(const std::string& account_address){
		order_.set_seller_address(account_address);
	}

	protocol::AssetKey const& OrderFrame::GetBuying() const{
		return order_.remain_order().buying();
	}

	protocol::AssetKey const& OrderFrame::GetSelling() const{
		return order_.remain_order().selling();
	}

	std::string OrderFrame::GetOrderID() const{
		return order_.remain_order().order_id();
	}

	void OrderFrame::SetOrderID(const std::string& order_id){
		order_.mutable_remain_order()->set_order_id(order_id);
	}

	uint32_t OrderFrame::GetFlags() const{
		return order_.flags();
	}

	int64_t OrderFrame::GetLastModified(){
		return order_.last_modified_ledger_seq();
	}

	std::string OrderFrame::GetTxHash() const{
		return order_.tx_hash();
	}

	int32_t OrderFrame::GetFeePercent(){
		return order_.mutable_remain_order()->fee_percent();
	}

	int64_t OrderFrame::GetFee(const int64_t& received){
		return received*GetFeePercent() / General::MATCH_FEE_BASE;
	}

	int64_t OrderFrame::GetBoughtAfterFee(const int64_t& received){
		return received - GetFee(received);
	}

	int32_t OrderFrame::GetOperationIndex(){
		return order_.op_index();
	}

	void OrderFrame::StoreDelete(Database& db) const{
		auto prep = db.GetPreparedStatement("DELETE FROM orders WHERE orderid=:s");
		auto& st = prep.statement();
		st.exchange(use(order_.remain_order().order_id()));
		st.define_and_bind();
		st.execute(true);
	}

	void OrderFrame::StoreChange(LedgerFrm* ledger,Database& db){
		StoreUpdateHelper(ledger,db, false);
	}

	void OrderFrame::StoreAdd(LedgerFrm* ledger, Database& db){
		StoreUpdateHelper(ledger,db, true);
	}

	double OrderFrame::ComputePrice() const{
		return double(order_.remain_order().price().n()) / double(order_.remain_order().price().d());
	}

	void OrderFrame::Touch(LedgerFrm* ledger){
		order_.set_last_modified_ledger_seq(ledger->GetClosingLedgerSeq());
	}

	void OrderFrame::StoreUpdateHelper(LedgerFrm* ledger, Database& db, bool insert){
		
		Touch(ledger);

		std::string sql;
		soci::indicator selling_ind = soci::i_ok, buying_ind = soci::i_ok;
		unsigned int sellingType = order_.remain_order().selling().type();
		unsigned int buyingType = order_.remain_order().buying().type();
		unsigned int flags = 1;
		if (insert){
			sql = "INSERT INTO orders (sellerid,orderid,"
				"sellingassettype,sellingassetcode,sellingissuer,"
				"buyingassettype,buyingassetcode,buyingissuer,"
				"amount,pricen,priced,price,flags,lastmodified,txhash,opindex) VALUES "
				"(:sid,:oid,:sat,:sac,:si,:bat,:bac,:bi,:a,:pn,:pd,:p,:f,:l,:ha,:opi)";
		}
		else{
			sql = "UPDATE orders SET sellingassettype=:sat "
				",sellingassetcode=:sac,sellingissuer=:si,"
				"buyingassettype=:bat,buyingassetcode=:bac,buyingissuer=:bi,"
				"amount=:a,pricen=:pn,priced=:pd,price=:p,flags=:f,"
				"lastmodified=:l,txhash=:ha,opindex:opi WHERE orderid=:oid";
		}

		auto prep = db.GetPreparedStatement(sql);
		auto& st = prep.statement();

		if (insert)
			st.exchange(use(order_.seller_address(), "sid"));

		st.exchange(use(order_.remain_order().order_id(), "oid"));
		st.exchange(use(sellingType, "sat"));
		st.exchange(use(order_.remain_order().selling().code(), selling_ind, "sac"));
		st.exchange(use(order_.remain_order().selling().issuer(), selling_ind, "si"));
		st.exchange(use(buyingType, "bat"));
		st.exchange(use(order_.remain_order().buying().code(), buying_ind, "bac"));
		st.exchange(use(order_.remain_order().buying().issuer(), buying_ind, "bi"));
		st.exchange(use(order_.remain_order().amount(), "a"));
		st.exchange(use(order_.remain_order().price().n(), "pn"));
		st.exchange(use(order_.remain_order().price().d(), "pd"));
		auto price = ComputePrice();
		st.exchange(use(price, "p"));
		st.exchange(use(flags, "f"));
		st.exchange(use(GetLastModified(), "l"));
		st.exchange(use(order_.tx_hash(), "ha"));
		st.exchange(use(order_.op_index(), "opi"));
		st.define_and_bind();

		//auto timer =insert ? db.getInsertTimer("offer") : db.getUpdateTimer("offer");
		st.execute(true);

		if (st.get_affected_rows() != 1)
		{
			throw std::runtime_error("could not update SQL");
		}
	}

	bool OrderFrame::Exists(Database& db, OrderKey const& key){

		int exists = 0;
		//auto timer = db.getSelectTimer("offer-exists");
		auto prep = db.GetPreparedStatement("SELECT EXISTS (SELECT NULL FROM orders "
			"WHERE sellerid=:id AND orderid=:s)");
		auto& st = prep.statement();
		st.exchange(use(key.account_address));
		st.exchange(use(key.order_id));
		st.exchange(into(exists));
		st.define_and_bind();
		st.execute(true);
		return exists != 0;
	}

	uint64_t OrderFrame::CountObjects(soci::session& sess){
		uint64_t count = 0;
		sess << "SELECT COUNT(*) FROM orders;", into(count);
		return count;
	}

	uint64_t OrderFrame::CountObjects(soci::session& sess, utils::LedgerRange const& ledgers){
		uint64_t count = 0;
		sess << "SELECT COUNT(*) FROM orders"
			" WHERE lastmodified >= :v1 AND lastmodified <= :v2;",
			into(count), use(ledgers.first()), use(ledgers.last());
		return count;
	}

	void DeleteOffersModifiedOnOrAfterLedger(Database& db, int64_t oldestLedger){
		/*db.getEntryCache().erase_if(
			[oldestLedger](std::shared_ptr<LedgerEntry const> le) -> bool {
			return le && le->data.type() == OFFER &&
			le->lastModifiedLedgerSeq >= oldestLedger;
			});*/

		{
			auto prep = db.GetPreparedStatement(
				"DELETE FROM orders WHERE lastmodified >= :v1");
			auto& st = prep.statement();
			st.exchange(soci::use(oldestLedger));
			st.define_and_bind();
			st.execute(true);
		}
	}

	OrderFrame::pointer OrderFrame::LoadOrder(std::string const& account_address,const std::string& order_id, Database& db){
		OrderFrame::pointer ret_order;

		std::string sql = orderColumnSelector;
		sql += " WHERE sellerid = :id AND orderid = :orderid";
		auto prep = db.GetPreparedStatement(sql);
		auto& st = prep.statement();
		st.exchange(use(account_address));
		st.exchange(use(order_id));

		//auto timer = db.getSelectTimer("offer");
		LoadOrders(prep, [&ret_order](protocol::Order const& order) {
			ret_order = std::make_shared<OrderFrame>(order);
		});

		return ret_order;
	}

	void OrderFrame::LoadOrders(StatementContext& prep, std::function<void(protocol::Order const&)> OrderProcessor){

		std::string account_address, order_id,tx_hash;
		uint64_t amount, last_modified_ledger_seq;
		int32_t price_n, price_d, flags;
		unsigned int selling_asset_type, buying_asset_type;
		std::string selling_asset_code, buying_asset_code, selling_issuer,buying_issuer;

		soci::indicator selling_asset_code_indicator, buying_asset_code_indicator,
			selling_issuer_indicator, buying_issuer_indicator;

		
		statement& st = prep.statement();
		st.exchange(into(account_address));
		st.exchange(into(order_id));
		st.exchange(into(selling_asset_type));
		st.exchange(into(selling_asset_code, selling_asset_code_indicator));
		st.exchange(into(selling_issuer, selling_issuer_indicator));
		st.exchange(into(buying_asset_type));
		st.exchange(into(buying_asset_code, buying_asset_code_indicator));
		st.exchange(into(buying_issuer, buying_issuer_indicator));
		st.exchange(into(amount));
		st.exchange(into(price_n));
		st.exchange(into(price_d));
		st.exchange(into(flags));
		st.exchange(into(last_modified_ledger_seq));
		st.exchange(into(tx_hash));
		st.define_and_bind();
		st.execute(true);

		
		//fee percent ...

		while (st.got_data()){
			protocol::Order order;
			order.set_seller_address(account_address);
			order.mutable_remain_order()->set_order_id(order_id);
			order.mutable_remain_order()->set_amount(amount);
			order.mutable_remain_order()->mutable_price()->set_n(price_n);
			order.mutable_remain_order()->mutable_price()->set_d(price_d);
			order.set_flags(flags);
			order.set_last_modified_ledger_seq(last_modified_ledger_seq);
			order.set_tx_hash(tx_hash);
			
			order.mutable_remain_order()->mutable_buying()->set_type((protocol::AssetKey_Type)buying_asset_type);
			order.mutable_remain_order()->mutable_selling()->set_type((protocol::AssetKey_Type)selling_asset_type);

			if (selling_asset_type != protocol::AssetKey_Type_SELF_COIN){
				if ((selling_asset_code_indicator != soci::i_ok) ||
					(selling_issuer_indicator != soci::i_ok)){
					LOG_ERROR("bad database state");
					throw std::runtime_error("bad database state");
				}
				order.mutable_remain_order()->mutable_selling()->set_code(selling_asset_code);
				order.mutable_remain_order()->mutable_selling()->set_issuer(selling_issuer);
			}

			if (buying_asset_type != protocol::AssetKey_Type_SELF_COIN){
				if ((buying_asset_code_indicator != soci::i_ok) ||
					(buying_issuer_indicator != soci::i_ok))
				{
					LOG_ERROR("bad database state");
					throw std::runtime_error("bad database state");
				}
				order.mutable_remain_order()->mutable_buying()->set_code(buying_asset_code);
				order.mutable_remain_order()->mutable_buying()->set_issuer(buying_issuer);
			}

			OrderProcessor(order);
			st.fetch();
		}
	}

	void OrderFrame::LoadBestOrders(size_t num_orders, size_t offset, protocol::AssetKey const& selling, protocol::AssetKey const& buying,
		std::vector<OrderFrame::pointer>& return_orders, Database& db){

		std::string sql = orderColumnSelector;

		bool use_selling_asset = false;
		bool use_buying_asset = false;

		if (selling.type() == protocol::AssetKey_Type_SELF_COIN){
			sql += " WHERE sellingassettype = 1 AND sellingissuer IS NULL";
		}
		else{
			use_selling_asset = true;
			sql += " WHERE sellingassetcode = :pcur AND sellingissuer = :pi";
		}

		if (buying.type() == protocol::AssetKey_Type_SELF_COIN)	{
			sql += " AND buyingassettype = 1 AND buyingissuer IS NULL";
		}
		else{
			use_buying_asset = true;
			sql += " AND buyingassetcode = :gcur AND buyingissuer = :gi";
		}

		sql += " ORDER BY price, orderid LIMIT :n OFFSET :o";

		auto prep = db.GetPreparedStatement(sql);
		auto& st = prep.statement();

		if (use_selling_asset){
			st.exchange(use(selling.code()));
			st.exchange(use(selling.issuer()));
		}

		if (use_buying_asset){
			st.exchange(use(buying.code()));
			st.exchange(use(buying.issuer()));
		}

		st.exchange(use(num_orders));
		st.exchange(use(offset));

		//auto timer = db.getSelectTimer("offer");
		LoadOrders(prep, [&return_orders](protocol::Order const& od) {
			return_orders.emplace_back(std::make_shared<OrderFrame>(od));
		});
	}

	// load all offers from the database (very slow) ; key AccountID
	std::unordered_map<std::string, std::vector<OrderFrame::pointer>> OrderFrame::LoadAllOrders(Database& db){
		std::unordered_map<std::string, std::vector<OrderFrame::pointer>> ret_orders;
		std::string sql = orderColumnSelector;
		sql += " ORDER BY sellerid";
		auto prep = db.GetPreparedStatement(sql);

		//auto timer = db.getSelectTimer("offer");
		LoadOrders(prep, [&ret_orders](protocol::Order const& of) {
			auto& this_user_orders = ret_orders[of.seller_address()];
			this_user_orders.emplace_back(std::make_shared<OrderFrame>(of));
		});
		return ret_orders;
	}

	void OrderFrame::DropAll(Database& db){
		db.GetSession() << "DROP TABLE IF EXISTS orders;";
		db.GetSession() << kSQLCreateStatement1;
		db.GetSession() << kSQLCreateStatement2;
		db.GetSession() << kSQLCreateStatement3;
		db.GetSession() << kSQLCreateStatement4;
	}

	void OrderFrame::Initialize(Database& db){
		int n = 0;
		db.GetSession() << "select count(*)  from sqlite_master where type='table' and name = 'orders';", soci::into(n);
		if (n == 0){
			db.GetSession() << kSQLCreateStatement1;
			db.GetSession() << kSQLCreateStatement2;
			db.GetSession() << kSQLCreateStatement3;
			db.GetSession() << kSQLCreateStatement4;
		}
	}

	std::string OrderFrame::ToString(){
		int64_t ledger_seq = 0;
		int32_t tx_index = 0, op_index = 0;
		utils::parseId(order_.remain_order().order_id(), ledger_seq, tx_index, op_index);

		std::string str = utils::String::Format("order seller(%s) order_id(" FMT_I64 ":%d:%d) selling(%s:%s:%d) buying(%s:%s:%d) amount(" FMT_I64 ") price(%d/%d) fee_percent(%d) tx_hash(%s) flag(%d) last_modify_ledger(" FMT_I64 ")",
			order_.seller_address().c_str(),
			ledger_seq,tx_index,op_index,
			order_.remain_order().selling().issuer().c_str(),
			order_.remain_order().selling().code().c_str(), 
			order_.remain_order().selling().type(), 
			order_.remain_order().buying().issuer().c_str(),
			order_.remain_order().buying().code().c_str(),
			order_.remain_order().buying().type(), 
			order_.remain_order().amount(),
			order_.remain_order().price().n(),
			order_.remain_order().price().d(),
			order_.remain_order().fee_percent(),
			order_.tx_hash().c_str(),
			order_.flags(),
			(int64_t)order_.last_modified_ledger_seq());
		
		return std::move(str);
	}
}

