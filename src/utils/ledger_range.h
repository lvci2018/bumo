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

#ifndef LEDGER_RANGE_H
#define LEDGER_RANGE_H

#include "common.h"

namespace utils {
	class LedgerRange final
	{
	public:
		LedgerRange(int64_t first, int64_t last);
		friend bool operator==(LedgerRange const& x, LedgerRange const& y);
		friend bool operator!=(LedgerRange const& x, LedgerRange const& y);

		int64_t	first() const{
			return first_;
		}
		int64_t	last() const{
			return last_;
		}

	private:
		int64_t first_;
		int64_t last_;
	};
}

#endif