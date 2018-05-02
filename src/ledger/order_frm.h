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
#include <common/socidb.h>
#include <unordered_map>
#include <utils/ledger_range.h>

namespace soci
{
	class session;
}

namespace bumo
{
	class LedgerFrm;

	class OrderFrame{
	protected:
		protocol::Order order_;

		struct OrderKey
		{
			std::string account_address;
			std::string order_id;
		};

		double ComputePrice() const;
		void StoreUpdateHelper(LedgerFrm* ledger, SociDb& db, bool insert);
		void Touch(LedgerFrm* ledger);
		
	public:
		typedef std::shared_ptr<OrderFrame> pointer;

		enum OrderFlags
		{
			PASSIVE_FLAG = 1
		};

		OrderFrame();
		OrderFrame(protocol::Order const& from);
		OrderFrame(OrderFrame const& from);
		OrderFrame& operator=(OrderFrame const& other);
		OrderFrame::pointer Copy() const {
			return std::make_shared<OrderFrame>(*this);
		}


		protocol::Price const& GetPrice() const;
		int64_t GetAmount() const;
		void SetAmount(int64_t amount);
		std::string const& GetSellerID() const;
		void SetSellerID(const std::string& account_address);
		protocol::AssetKey const& GetBuying() const;
		protocol::AssetKey const& GetSelling() const;
		std::string GetOrderID() const;
		void SetOrderID(const std::string& order_id);
		uint32_t GetFlags() const;
		int64_t GetLastModified();
		std::string GetTxHash() const;
		int32_t GetFeePercent();
		int64_t GetFee(const int64_t& received);
		int64_t GetBoughtAfterFee(const int64_t& received);
		std::string ToString();
		int32_t GetOperationIndex();

		protocol::Order const&	GetOrder() const{
			return order_;
		}
		protocol::Order& GetOrder(){
			return order_;
		}

		void StoreDelete(SociDb& db) const;
		void StoreChange(LedgerFrm* ledger, SociDb& db);
		void StoreAdd(LedgerFrm* ledger, SociDb& db);
		static bool Exists(SociDb& db, OrderKey const& key);
		
		static uint64_t CountObjects(soci::session& sess);
		static uint64_t CountObjects(soci::session& sess, utils::LedgerRange const& ledgers);
		static void DeleteOffersModifiedOnOrAfterLedger(SociDb& db, int64_t oldestLedger);

		static OrderFrame::pointer LoadOrder(std::string const& account_address, const std::string& order_id, SociDb& db);
		static void	LoadOrders(StatementContext& prep, std::function<void(protocol::Order const&)> OrderProcessor);

		static void LoadBestOrders(size_t num_orders, size_t offset, protocol::AssetKey const& pays, protocol::AssetKey const& gets, 
			std::vector<OrderFrame::pointer>& return_orders, SociDb& db);

		// load all offers from the database (very slow) ; key AccountID
		static std::unordered_map<std::string, std::vector<OrderFrame::pointer>> LoadAllOrders(SociDb& db);

		static void DropAll(SociDb& db);
		static void Initialize(SociDb& db);
	private:
		static const char* kSQLCreateStatement1;
		static const char* kSQLCreateStatement2;
		static const char* kSQLCreateStatement3;
		static const char* kSQLCreateStatement4;

	};

}

#endif // !ORDER_FRAME_H
