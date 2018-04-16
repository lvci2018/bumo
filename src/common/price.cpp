#include "price.h"

namespace bumo {

	bool operator>=(protocol::Price const& a, protocol::Price const& b){
		uint128_t l(a.n());
		uint128_t r(a.d());
		l *= b.d();
		r *= b.n();
		return l >= r;
	}

	bool operator>(protocol::Price const& a, protocol::Price const& b){
		uint128_t l(a.n());
		uint128_t r(a.d());
		l *= b.d();
		r *= b.n();
		return l > r;
	}

	bool operator==(protocol::Price const& a, protocol::Price const& b){
		return (a.n() == b.n()) && (a.d() == b.d());
	}

}