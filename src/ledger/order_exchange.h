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

	ExchangeResult Exchange(int64_t wheatReceived, const protocol::Price& price,
		int64_t maxWheatReceive, int64_t maxSheepSend);

	class OrderExchange
	{

		std::vector<protocol::ClaimOrder> order_trail_;

	public:
		OrderExchange();

		// buys wheat with sheep from a single order //用羊买小麦从一个单子
		enum CrossOrderResult
		{
			eOrderPartial,
			eOrderTaken,
			eOrderCantConvert
		};

		CrossOrderResult CrossOrder(OrderFrame& sellingWheatOrder,
			int64_t maxWheatReceived,
			int64_t& numWheatReceived, int64_t maxSheepSend,
			int64_t& numSheepSent);

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
			protocol::AssetKey const& sheep, int64_t maxSheepSent, int64_t& sheepSend,
			protocol::AssetKey const& wheat, int64_t maxWheatReceive, int64_t& weatReceived,
			std::function<OrderFilterResult(OrderFrame const&)> filter);

		std::vector<protocol::ClaimOrder> const& GetOrderTrail() const{
			return order_trail_;
		}
	};

}

#endif // !ORDER_EXCHANGE_H

