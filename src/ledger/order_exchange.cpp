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
#include "order_exchange.h"
#include <utils/base_int.h>
#include <common/storage.h>
#include "account.h"
#include <algorithm>
#include "environment.h"
#include "ledger_manager.h"

namespace bumo {


	namespace{

		int64_t CanBuyAtMost(const protocol::AssetKey& asset, const protocol::Price& price){
			if (asset.type() == protocol::AssetKey_Type_SELF_COIN)
				return INT64_MAX;

			// compute value based on what the account can receive
			auto seller_max_sheep = INT64_MAX;

			auto result = int64_t{};
			if (!utils::bigDivide(result, seller_max_sheep, price.d(), price.n(), utils::Rounding::eRoundDown))
				result = INT64_MAX;

			return result;
		}

		int64_t	CanSellAtMost(AccountFrm::pointer account, const protocol::AssetKey& asset){
			if (asset.type() == protocol::AssetKey_Type_SELF_COIN)
				// can only send above the minimum balance
				return account->GetBalanceAboveReserve();

			protocol::AssetStore asset_s;
			if(account->GetAsset(asset, asset_s))
				return asset_s.amount();

			return 0;
		}
	}

	ExchangeResult Exchange(int64_t wheat_received, const protocol::Price& price, int64_t max_wheat_receive, int64_t max_sheep_send){
		auto result = ExchangeResult{};
		result.reduced_ = wheat_received > max_wheat_receive;
		result.num_wheat_received_ = (std::min)(wheat_received, max_wheat_receive);

		// this guy can get X wheat to you. How many sheep does that get him?
		// bias towards seller
		if (!utils::bigDivide(result.num_sheep_send_, result.num_wheat_received_, price.n(),price.d(), utils::Rounding::eRoundUp)){
			result.reduced_ = true;
			result.num_sheep_send_ = INT64_MAX;
		}

		result.reduced_ = result.reduced_ || (result.num_sheep_send_ > max_sheep_send);
		result.num_sheep_send_ = (std::min)(result.num_sheep_send_, max_sheep_send);

		auto new_wheat_received = int64_t{};
		if (!utils::bigDivide(new_wheat_received, result.num_sheep_send_, price.d(), price.n(), utils::Rounding::eRoundDown))
			new_wheat_received = INT64_MAX;

		result.num_wheat_received_ = (std::min)(result.num_wheat_received_, new_wheat_received);
		return result;
	}

	OrderExchange::CrossOrderResult OrderExchange::CrossOrder(OrderFrame& selling_wheat_order,
		int64_t max_wheat_receive, int64_t& num_wheat_received,
		int64_t max_sheep_send, int64_t& num_sheep_sent){

		assert(max_wheat_receive > 0);
		assert(max_sheep_send > 0);

		const protocol::AssetKey& sheep = selling_wheat_order.GetBuying();
		const protocol::AssetKey& wheat = selling_wheat_order.GetSelling();

		const std::string& account_id_b = selling_wheat_order.GetSellerID();

		Database& db = Storage::Instance().lite_db();

		AccountFrm::pointer account_b;
		if (!environment_->GetEntry(account_id_b, account_b))
			PROCESS_EXIT("Account(%s) must be exist,invalid database state, order must have matching account", account_id_b.c_str());


		num_wheat_received = (std::min)({
			CanBuyAtMost(sheep, selling_wheat_order.GetPrice()),
			CanSellAtMost(account_b, wheat),
			selling_wheat_order.GetAmount()
		});

		//assert(num_wheat_received >= 0);
		if (num_wheat_received<0)
			PROCESS_EXIT("num_wheat_received(" FMT_I64 ") CanBuyAtMost(" FMT_I64 ") CanSellAtMost(" FMT_I64 ") selling_wheat_order amount(" FMT_I64 ")", 
			num_wheat_received, CanBuyAtMost(sheep, selling_wheat_order.GetPrice()), CanSellAtMost(account_b, wheat), selling_wheat_order.GetAmount());

		LOG_INFO("num_wheat_received(" FMT_I64 ") CanBuyAtMost(" FMT_I64 ") CanSellAtMost(" FMT_I64 ") selling_wheat_order amount(" FMT_I64 ")",
			num_wheat_received, CanBuyAtMost(sheep, selling_wheat_order.GetPrice()), CanSellAtMost(account_b, wheat), selling_wheat_order.GetAmount());

		if (selling_wheat_order.GetAmount() != num_wheat_received){
			LOG_INFO("Order(%s) reset amount, orig amount(" FMT_I64 ")  new amount(" FMT_I64 ")", selling_wheat_order.GetOrderID().c_str(), selling_wheat_order.GetAmount(), num_wheat_received);
		}

		selling_wheat_order.SetAmount(num_wheat_received);
		

		auto exchangeResult = Exchange(num_wheat_received, selling_wheat_order.GetPrice(), max_wheat_receive, max_sheep_send);
		//reset amount of deal
		num_wheat_received = exchangeResult.num_wheat_received_;
		num_sheep_sent = exchangeResult.num_sheep_send_;

		bool order_taken = false;

		switch (exchangeResult.Type()){
		case ExchangeResultType::REDUCED_TO_ZERO:
			return eOrderCantConvert;
		case ExchangeResultType::BOGUS:
			// force delete the offer as it represents a bogus offer
			num_wheat_received = 0;
			num_sheep_sent = 0;
			order_taken = true;
			break;
		default:
			break;
		}

		order_taken = order_taken || selling_wheat_order.GetAmount() <= num_wheat_received;
		if (order_taken){ // entire offer is taken
			selling_wheat_order.StoreDelete(db);
		}
		else{
			int64_t new_amount = selling_wheat_order.GetAmount() - num_wheat_received;
			selling_wheat_order.SetAmount(new_amount);
			selling_wheat_order.StoreChange(ledger_, db);
		}

		// Adjust balances
		if (num_sheep_sent != 0){
			//fee compulate
			int64_t fee = selling_wheat_order.GetFee(num_sheep_sent);
			int64_t num_sheep_sent_after_fee = num_sheep_sent - fee;
			/*if (!AccountFrm::PayIssuerFee(environment_, sheep, fee)){
				PROCESS_EXIT();
			}*/

			if (sheep.type() == protocol::AssetKey_Type_SELF_COIN){
				if (!account_b->AddBalance(num_sheep_sent)){
					return eOrderCantConvert;
				}
			}
			else{
				protocol::AssetStore asset_s;
				account_b->GetAsset(sheep, asset_s);
				int64_t new_amount = asset_s.amount() + num_sheep_sent;
				if (new_amount < 0){
					return eOrderCantConvert;
				}
				asset_s.set_amount(new_amount);
				account_b->SetAsset(asset_s);
			}
		}

		if (num_wheat_received != 0){
			if (wheat.type() == protocol::AssetKey_Type_SELF_COIN){
				if (account_b->AddBalance(-num_wheat_received)){
					return eOrderCantConvert;
				}
			}
			else{
				protocol::AssetStore asset_w;
				account_b->GetAsset(wheat, asset_w);
				int64_t new_amount = asset_w.amount() - num_wheat_received;
				if (new_amount < 0){
					return eOrderCantConvert;
				}
				asset_w.set_amount(new_amount);
				account_b->SetAsset(asset_w);
			}
		}

		protocol::ClaimOrder co;
		co.set_seller_id(account_b->GetAccountAddress());
		co.set_order_id(selling_wheat_order.GetOrderID());
		co.set_tx_hash(selling_wheat_order.GetTxHash());
		co.mutable_price()->CopyFrom(selling_wheat_order.GetPrice());

		co.mutable_asset_sold()->CopyFrom(wheat);
		co.set_amount_sold(num_wheat_received);
		co.mutable_asset_bought()->CopyFrom(sheep);
		co.set_amount_bought(num_sheep_sent);
		order_trail_.push_back(co);

		return order_taken ? eOrderTaken : eOrderPartial;
	}

	OrderExchange::ConvertResult OrderExchange::ConvertWithOrders(
	protocol::AssetKey const& sheep, int64_t max_sheep_send, int64_t& sheep_sent,
	protocol::AssetKey const& wheat, int64_t max_wheat_receive, int64_t& wheat_received,
		std::function<OrderFilterResult(OrderFrame const&)> filter){

		sheep_sent = 0;
		wheat_received = 0;

		Database& db = Storage::Instance().lite_db();

		size_t order_offset = 0;

		bool need_more = (max_wheat_receive > 0 && max_sheep_send > 0);

		while (need_more){
			std::vector<OrderFrame::pointer> retList;
			OrderFrame::LoadBestOrders(5, order_offset, wheat, sheep, retList, db);

			order_offset += retList.size();

			for (auto& wheat_order : retList){
				/*LOG_INFO("counter order:account(%s) selling(%s) buying(%s) price(%d/%d)", wheat_order->GetSellerID().c_str(),
					wheat_order->GetSelling().code().c_str(), wheat_order->GetBuying().code().c_str(), wheat_order->GetPrice().n(), wheat_order->GetPrice().d());*/
				if (filter){
					OrderFilterResult r = filter(*wheat_order);
					switch (r){
					case eKeep:
						break;
					case eStop:
						return eFilterStop;
					case eSkip:
						continue;
					}
				}

				int64_t num_wheat_received=0;
				int64_t num_sheep_sent=0;

				std::string order_desc = wheat_order->ToString();
				LOG_INFO("counter %s max_wheat_receive(" FMT_I64"),max_sheep_send(" FMT_I64")", order_desc.c_str(), max_wheat_receive, max_sheep_send);

				OrderExchange::CrossOrderResult cor =
					CrossOrder(*wheat_order, max_wheat_receive, num_wheat_received,
					max_sheep_send, num_sheep_sent);

				LOG_INFO("counter %s match result:%d num_wheat_received(" FMT_I64"),num_sheep_sent(" FMT_I64")",
					order_desc.c_str(), (int)cor, num_wheat_received, num_sheep_sent);

				assert(num_sheep_sent >= 0);
				assert(num_sheep_sent <= max_sheep_send);
				assert(num_wheat_received >= 0);
				assert(num_wheat_received <= max_wheat_receive);

				switch (cor){
				case eOrderTaken:
					assert(order_offset > 0);
					order_offset--; // adjust offset as an offer was deleted
					break;
				case eOrderPartial:
					break;
				case eOrderCantConvert:
					return ePartial;
				}

				sheep_sent += num_sheep_sent;
				max_sheep_send -= num_sheep_sent;

				wheat_received += num_wheat_received;
				max_wheat_receive -= num_wheat_received;

				need_more = (max_wheat_receive > 0 && max_sheep_send > 0);
				if (!need_more)
					return eOK;
				else if (cor == eOrderPartial)
					return ePartial;

			}

			// still stuff to fill but no more offers
			if (need_more && retList.size() < 5)
				return eOK;
		}
		return eOK;
	}

	
}