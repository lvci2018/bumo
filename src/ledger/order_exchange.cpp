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
#include <algorithm>

namespace bumo {


	ExchangeResult Exchange(int64_t wheatReceived, const protocol::Price& price, int64_t maxWheatReceive, int64_t maxSheepSend){
		auto result = ExchangeResult{};
		result.reduced_ = wheatReceived > maxWheatReceive;
		result.num_wheat_received_ = std::min(wheatReceived,maxWheatReceive);

		// this guy can get X wheat to you. How many sheep does that get him?
		// bias towards seller
		if (!utils::bigDivide(result.num_sheep_send_, result.num_wheat_received_, price.n(),price.d(), utils::Rounding::eRoundUp))
		{
			result.reduced_ = true;
			result.num_sheep_send_ = INT64_MAX;
		}

		result.reduced_ = result.reduced_ || (result.num_sheep_send_ > maxSheepSend);
		result.num_sheep_send_ = std::min(result.num_sheep_send_,maxSheepSend);

		auto newWheatReceived = int64_t{};
		if (!utils::bigDivide(newWheatReceived, result.num_sheep_send_, price.d(), price.n(),utils::Rounding::eRoundDown)){
			newWheatReceived = INT64_MAX;
		}

		result.num_wheat_received_ =std::min(result.numWheatReceived,newWheatReceived);
		return result;
	}


	OrderExchange::CrossOrderResult OrderExchange::CrossOrder(OrderFrame& sellingWheatOrder,
		int64_t maxWheatReceived,
		int64_t& numWheatReceived, int64_t maxSheepSend,
		int64_t& numSheepSent){

		//todo
	}

	OrderExchange::ConvertResult OrderExchange::ConvertWithOrders(
		protocol::AssetKey const& sheep, int64_t maxSheepSent, int64_t& sheepSend,
		protocol::AssetKey const& wheat, int64_t maxWheatReceive, int64_t& weatReceived,
		std::function<OrderFilterResult(OrderFrame const&)> filter){

		//todo
	}
}