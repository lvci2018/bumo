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

namespace bumo {


	ExchangeResult Exchange(int64_t wheatReceived, protocol::Price price,
		int64_t maxWheatReceive, int64_t maxSheepSend){
		auto result = ExchangeResult{};

		//todo ...


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