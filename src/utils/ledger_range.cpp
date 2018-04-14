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

#include "ledger_range.h"

namespace utils {


	LedgerRange::LedgerRange(int64_t first, int64_t last)
		: first_{ first }, last_{ last }{
		assert(first_ > 0);
		assert(last_ >= first_);
	}

	bool operator==(LedgerRange const& x, LedgerRange const& y) {
		if (x.first_ != y.first_)
			return false;

		if (x.last_ != y.last_)
			return false;

		return true;
	}

	bool operator!=(LedgerRange const& x, LedgerRange const& y) {
		return !(x == y);
	}

}