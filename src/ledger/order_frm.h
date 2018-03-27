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

#ifndef ORDER_FRAME_H
#define ORDER_FRAME_H

#include <proto/cpp/chain.pb.h>
#include <common/database.h>
#include <unordered_map>

namespace soci
{
	class session;
}

namespace bumo
{

	class OrderFrame{
	protected:
		protocol::Order order_;

		struct OrderKey
		{
			std::string account_address;
			int64_t order_id;
		};

		double ComputePrice() const;
		void StoreUpdateHelper(Database& db, bool insert);
	public:
		typedef std::shared_ptr<OrderFrame> pointer;

		OrderFrame();
		OrderFrame(protocol::Order const& from);
		OrderFrame(OrderFrame const& from);
		OrderFrame& operator=(OrderFrame const& other);
		OrderFrame::pointer Copy() const {
			return std::make_shared<OrderFrame>(*this);
		}


		protocol::Price const& GetPrice() const;
		int64_t GetAmount() const;
		std::string const& GetSellerID() const;
		protocol::AssetKey const& GetBuying() const;
		protocol::AssetKey const& GetSelling() const;
		uint64_t GetOrderID() const;

		protocol::Order const&	GetOrder() const{
			return order_;
		}
		protocol::Order& GetOrder(){
			return order_;
		}

		void StoreDelete(Database& db) const;
		void StoreChange(Database& db);
		void StoreAdd(Database& db);
		static bool Exists(Database& db, OrderKey const& key);
		static uint64_t CuntObjects(soci::session& sess);
		//static void deleteOffersModifiedOnOrAfterLedger(Database& db,uint32_t oldestLedger);
		static OrderFrame::pointer LoadOffer(std::string const& account_address, uint64_t order_id, Database& db);
		static void	LoadOffers(StatementContext& prep, std::function<void(protocol::Order const&)> OrderProcessor);

		static void LoadBestOffers(size_t num_orders, size_t offset, protocol::AssetKey const& pays, protocol::AssetKey const& gets, 
			std::vector<OrderFrame::pointer>& return_orders, Database& db);

		// load all offers from the database (very slow) ; key AccountID
		static std::unordered_map<std::string, std::vector<OrderFrame::pointer>> LoadAllOffers(Database& db);

		static void DropAll(Database& db);
	private:
		static const char* kSQLCreateStatement1;
		static const char* kSQLCreateStatement2;
		static const char* kSQLCreateStatement3;
		static const char* kSQLCreateStatement4;

	};

}

#endif // !ORDER_FRAME_H
