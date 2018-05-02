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

#ifndef ORDER_EXCHANGE_H
#define ORDER_EXCHANGE_H

#include <utils/common.h>
#include <proto/cpp/chain.pb.h>
#include "order_frm.h"

namespace bumo {

	class Environment;

	enum class ExchangeResultType
	{
		NORMAL,
		REDUCED_TO_ZERO,
		BOGUS
	};

	struct ExchangeResult
	{
		int64_t num_wheat_received_;
		int64_t num_sheep_send_;
		bool reduced_;

		ExchangeResultType	Type() const
		{
			if (num_wheat_received_ != 0 && num_sheep_send_ != 0)
				return ExchangeResultType::NORMAL;
			else
				return reduced_ ? ExchangeResultType::REDUCED_TO_ZERO : ExchangeResultType::BOGUS;
		}
	};

	ExchangeResult Exchange(int64_t wheat_received, const protocol::Price& price,
		int64_t max_wheat_receive, int64_t max_sheep_send);

	class OrderExchange
	{
		OrderFrame::pointer sell_sheep_order_;
		std::string sell_sheep_order_flag_;
		std::vector<protocol::ClaimOrder> order_trail_;
		std::shared_ptr<Environment> environment_;
		LedgerFrm* ledger_;
	public:
		OrderExchange(LedgerFrm* ledger, std::shared_ptr<Environment> environment, OrderFrame::pointer sell_sheep_order, const std::string& sell_sheep_order_flag) :
			ledger_(ledger), environment_(environment), sell_sheep_order_(sell_sheep_order), sell_sheep_order_flag_(sell_sheep_order_flag){}

		// buys wheat with sheep from a single order //用羊买小麦从一个单子
		enum CrossOrderResult
		{
			eOrderPartial,
			eOrderTaken,
			eOrderCantConvert
		};

		CrossOrderResult CrossOrder(OrderFrame& selling_wheat_order,
			int64_t max_wheat_receive,int64_t& num_wheat_received, 
			int64_t max_sheep_send,int64_t& num_sheep_sent);

		enum OrderFilterResult
		{
			eKeep,
			eStop,
			eSkip
		};

		enum ConvertResult
		{
			eOK,
			ePartial,
			eFilterStop
		};
		// buys wheat with sheep, crossing as many offers as necessary
		ConvertResult ConvertWithOrders(
			protocol::AssetKey const& sheep, int64_t max_sheep_send, int64_t& sheep_sent,
			protocol::AssetKey const& wheat, int64_t max_wheat_receive, int64_t& wheat_received,
			std::function<OrderFilterResult(OrderFrame const&)> filter);

		std::vector<protocol::ClaimOrder> const& GetOrderTrail() const{
			return order_trail_;
		}

	};

}

#endif // !ORDER_EXCHANGE_H

