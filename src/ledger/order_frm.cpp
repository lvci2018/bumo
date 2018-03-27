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
using namespace soci;

namespace bumo {

	const char* OrderFrame::kSQLCreateStatement1 =
		"CREATE TABLE orders"
		"("
		"sellerid         VARCHAR(56)  NOT NULL,"
		"orderid          BIGINT       NOT NULL CHECK (orderid >= 0),"
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
		"PRIMARY KEY      (offerid)"
		");";

	const char* OrderFrame::kSQLCreateStatement2 =
		"CREATE INDEX sellingissuerindex ON orders (sellingissuer);";

	const char* OrderFrame::kSQLCreateStatement3 =
		"CREATE INDEX buyingissuerindex ON orders (buyingissuer);";

	const char* OrderFrame::kSQLCreateStatement4 =
		"CREATE INDEX priceindex ON orders (price);";

	static const char* offerColumnSelector =
		"SELECT sellerid,orderid,sellingassettype,sellingassetcode,sellingissuer,"
		"buyingassettype,buyingassetcode,buyingissuer,amount,pricen,priced,"
		"flags,lastmodified "
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
		return order_.order().price();
	}

	int64_t OrderFrame::GetAmount() const{
		return order_.order().amount();
	}

	std::string const& OrderFrame::GetSellerID() const{
		return order_.seller_address();
	}

	protocol::AssetKey const& OrderFrame::GetBuying() const{
		return order_.order().buying();
	}

	protocol::AssetKey const& OrderFrame::GetSelling() const{
		return order_.order().selling();
	}

	uint64_t OrderFrame::GetOrderID() const{
		return order_.order().order_id();
	}

	void OrderFrame::StoreDelete(Database& db) const{
		auto prep = db.getPreparedStatement("DELETE FROM orders WHERE orderid=:s");
		auto& st = prep.statement();
		st.exchange(use(order_.order().order_id()));
		st.define_and_bind();
		st.execute(true);
	}

	void OrderFrame::StoreChange(Database& db){
		StoreUpdateHelper(db, false);
	}

	void OrderFrame::StoreAdd(Database& db){
		StoreUpdateHelper(db, true);
	}

	double OrderFrame::ComputePrice() const{
		return double(order_.order().price().n()) / double(order_.order().price().d());
	}

	void OrderFrame::StoreUpdateHelper(Database& db, bool insert){
		
		std::string sql;
		soci::indicator selling_ind = soci::i_ok, buying_ind = soci::i_ok;

		if (insert)
		{
			sql = "INSERT INTO orders (sellerid,orderid,"
				"sellingassettype,sellingassetcode,sellingissuer,"
				"buyingassettype,buyingassetcode,buyingissuer,"
				"amount,pricen,priced,price,flags) VALUES "			//"amount,pricen,priced,price,flags,lastmodified) VALUES "
				"(:sid,:oid,:sat,:sac,:si,:bat,:bac,:bi,:a,:pn,:pd,:p,:f,:l)";
		}
		else
		{
			sql = "UPDATE orders SET sellingassettype=:sat "
				",sellingassetcode=:sac,sellingissuer=:si,"
				"buyingassettype=:bat,buyingassetcode=:bac,buyingissuer=:bi,"
				"amount=:a,pricen=:pn,priced=:pd,price=:p,flags=:f,"
				"WHERE orderid=:oid";								//"lastmodified=:l WHERE order_id=:oid"
		}

		auto prep = db.getPreparedStatement(sql);
		auto& st = prep.statement();

		if (insert)
		{
			st.exchange(use(order_.seller_address(), "sid"));
		}
		st.exchange(use(order_.order().order_id(), "oid"));
		st.exchange(use(order_.order().selling().type(), "sat"));
		st.exchange(use(order_.order().selling().code(), selling_ind, "sac"));
		st.exchange(use(order_.order().selling().issuer(), selling_ind, "si"));
		st.exchange(use(order_.order().buying().type(), "bat"));
		st.exchange(use(order_.order().buying().code(), buying_ind, "bac"));
		st.exchange(use(order_.order().buying().issuer(), buying_ind, "bi"));
		st.exchange(use(order_.order().amount(), "a"));
		st.exchange(use(order_.order().price().n(), "pn"));
		st.exchange(use(order_.order().price().d(), "pd"));
		auto price = ComputePrice();
		st.exchange(use(price, "p"));
		st.exchange(use(1, "f"));
		//st.exchange(use(getLastModified(), "l"));
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
		auto prep = db.getPreparedStatement("SELECT EXISTS (SELECT NULL FROM orders "
			"WHERE sellerid=:id AND orderid=:s)");
		auto& st = prep.statement();
		st.exchange(use(key.account_address));
		st.exchange(use(key.order_id));
		st.exchange(into(exists));
		st.define_and_bind();
		st.execute(true);
		return exists != 0;
	}

	uint64_t OrderFrame::CuntObjects(soci::session& sess){
		uint64_t count = 0;
		sess << "SELECT COUNT(*) FROM orders;", into(count);
		return count;
	}

	OrderFrame::pointer OrderFrame::LoadOffer(std::string const& account_address, uint64_t order_id, Database& db){
		OrderFrame::pointer ret_order;

		std::string sql = offerColumnSelector;
		sql += " WHERE sellerid = :id AND orderid = :orderid";
		auto prep = db.getPreparedStatement(sql);
		auto& st = prep.statement();
		st.exchange(use(account_address));
		st.exchange(use(order_id));

		//auto timer = db.getSelectTimer("offer");
		LoadOffers(prep, [&ret_order](protocol::Order const& order) {
			ret_order = std::make_shared<OrderFrame>(order);
		});

		return ret_order;
	}

	void OrderFrame::LoadOffers(StatementContext& prep, std::function<void(protocol::Order const&)> OrderProcessor){

		std::string account_address;
		uint64_t order_id, amount;
		int32_t price_n, price_d;
		unsigned int sellingAssetType, buyingAssetType;
		std::string sellingAssetCode, buyingAssetCode, sellingIssuerStrKey,buyingIssuerStrKey;

		soci::indicator sellingAssetCodeIndicator, buyingAssetCodeIndicator,
			sellingIssuerIndicator, buyingIssuerIndicator;

		protocol::Order order;
		statement& st = prep.statement();
		st.exchange(into(account_address));
		st.exchange(into(order_id));
		st.exchange(into(sellingAssetType));
		st.exchange(into(sellingAssetCode, sellingAssetCodeIndicator));
		st.exchange(into(sellingIssuerStrKey, sellingIssuerIndicator));
		st.exchange(into(buyingAssetType));
		st.exchange(into(buyingAssetCode, buyingAssetCodeIndicator));
		st.exchange(into(buyingIssuerStrKey, buyingIssuerIndicator));
		st.exchange(into(amount));
		st.exchange(into(price_n));
		st.exchange(into(price_d));
		//st.exchange(into(oe.flags));
		//st.exchange(into(le.lastModifiedLedgerSeq));
		st.define_and_bind();
		st.execute(true);
		while (st.got_data()){
			order.set_seller_address(account_address);
			
			order.mutable_order()->mutable_buying()->set_type(buyingAssetType);
			order.mutable_order()->mutable_selling()->set_type(sellingAssetType);

			if ((sellingAssetCodeIndicator != soci::i_ok) ||
				(sellingIssuerIndicator != soci::i_ok))
			{
				LOG_ERROR("bad database state");
				throw std::runtime_error("bad database state");
			}
			order.mutable_order()->mutable_selling()->set_code(sellingAssetCode);
			order.mutable_order()->mutable_selling()->set_issuer(sellingIssuerStrKey);

			if ((buyingAssetCodeIndicator != soci::i_ok) ||
				(buyingIssuerIndicator != soci::i_ok))
			{
				LOG_ERROR("bad database state");
				throw std::runtime_error("bad database state");
			}
			order.mutable_order()->mutable_buying()->set_code(buyingAssetCode);
			order.mutable_order()->mutable_buying()->set_issuer(buyingIssuerStrKey);

			OrderProcessor(order);
			st.fetch();
		}
	}

	void OrderFrame::LoadBestOffers(size_t num_orders, size_t offset, protocol::AssetKey const& selling, protocol::AssetKey const& buying,
		std::vector<OrderFrame::pointer>& return_orders, Database& db){

		std::string sql = offerColumnSelector;

		bool useSellingAsset = false;
		bool useBuyingAsset = false;

		if (selling.type() == 0){
			sql += " WHERE sellingassettype = 0 AND sellingissuer IS NULL";
		}
		else{
			useSellingAsset = true;
			sql += " WHERE sellingassetcode = :pcur AND sellingissuer = :pi";
		}

		if (buying.type() == 0)	{
			sql += " AND buyingassettype = 0 AND buyingissuer IS NULL";
		}
		else{
			useBuyingAsset = true;
			sql += " AND buyingassetcode = :gcur AND buyingissuer = :gi";
		}

		sql += " ORDER BY price, offerid LIMIT :n OFFSET :o";

		auto prep = db.getPreparedStatement(sql);
		auto& st = prep.statement();

		if (useSellingAsset){
			st.exchange(use(selling.code()));
			st.exchange(use(selling.issuer()));
		}

		if (useBuyingAsset){
			st.exchange(use(buying.code()));
			st.exchange(use(buying.issuer()));
		}

		st.exchange(use(num_orders));
		st.exchange(use(offset));

		//auto timer = db.getSelectTimer("offer");
		LoadOffers(prep, [&return_orders](protocol::Order const& od) {
			return_orders.emplace_back(std::make_shared<OrderFrame>(od));
		});
	}

	// load all offers from the database (very slow) ; key AccountID
	std::unordered_map<std::string, std::vector<OrderFrame::pointer>> OrderFrame::LoadAllOffers(Database& db){
		std::unordered_map<std::string, std::vector<OrderFrame::pointer>> retOffers;
		std::string sql = offerColumnSelector;
		sql += " ORDER BY sellerid";
		auto prep = db.getPreparedStatement(sql);

		//auto timer = db.getSelectTimer("offer");
		LoadOffers(prep, [&retOffers](protocol::Order const& of) {
			auto& thisUserOffers = retOffers[of.seller_address()];
			thisUserOffers.emplace_back(std::make_shared<OrderFrame>(of));
		});
		return retOffers;
	}

	void OrderFrame::DropAll(Database& db){
		db.getSession() << "DROP TABLE IF EXISTS orders;";
		db.getSession() << kSQLCreateStatement1;
		db.getSession() << kSQLCreateStatement2;
		db.getSession() << kSQLCreateStatement3;
		db.getSession() << kSQLCreateStatement4;
	}
}

