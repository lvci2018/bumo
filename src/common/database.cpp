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

#include "database.h"
#include <main/configure.h>
#include <utils/logger.h>
#include <ledger/order_frm.h>

#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

extern "C" void register_factory_sqlite3();

#ifdef USE_POSTGRES
extern "C" void register_factory_postgresql();
#endif

// NOTE: soci will just crash and not throw
//  if you misname a column in a query. yay!

namespace bumo
{

	using namespace soci;
	using namespace std;

	bool Database::gDriversRegistered = false;

	static void SetSerializable(soci::session& sess){
		sess << "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
			"SERIALIZABLE";
	}

	void Database::RegisterDrivers() {
		if (!gDriversRegistered)
		{
			register_factory_sqlite3();
#ifdef USE_POSTGRES
			register_factory_postgresql();
#endif
			gDriversRegistered = true;
		}
	}

	Database::Database(const std::string& connect_string) {
		RegisterDrivers();

		LOG_INFO("Connecting to: %s", connect_string.c_str());

		try{
			std::string connect_str = "sqlite3://" + connect_string;
			session_.open(connect_str);
		}
		catch (std::exception e){
			PROCESS_EXIT("Sqlite database connect path(%s) error", connect_string.c_str());
		}

		connect_string_ = "sqlite3://" + connect_string;;
		if (IsSqlite()){
			session_ << "PRAGMA journal_mode = WAL";
			// busy_timeout gives room for external processes
			// that may lock the database for some time
			session_ << "PRAGMA busy_timeout = 10000";
		}
		else{
			SetSerializable(session_);
		}
	}

	

	void Database::SetCurrentTransactionReadOnly(){
		if (!IsSqlite()){
			auto prep = GetPreparedStatement("SET TRANSACTION READ ONLY");
			auto& st = prep.statement();
			st.define_and_bind();
			st.execute(false);
		}
	}

	bool Database::IsSqlite() const	{
		return connect_string_.find("sqlite3:") !=
			std::string::npos;
	}

	bool Database::CanUsePool() const {
		return !(connect_string_ == ("sqlite3://:memory:"));
	}

	void Database::ClearPreparedStatementCache(){
		// Flush all prepared statements; in sqlite they represent open cursors
		// and will conflict with any DROP TABLE commands issued below
		for (auto st : statements_)	{
			st.second->clean_up(true);
		}
		statements_.clear();
		//mStatementsSize.set_count(mStatements.size());
	}

	void Database::Initialize()	{
		ClearPreparedStatementCache();

		OrderFrame::DropAll(*this);
		//OrderFrame::Initialize(*this);
	}

	soci::session& Database::GetSession(){
		// global session can only be used from the main thread
		//assertThreadIsMain();		// maybe err , todo
		return session_;
	}

	soci::connection_pool&	Database::GetPool()	{
		if (!pool_){
			auto const& c = connect_string_;
			if (!CanUsePool()){
				std::string s("Can't create connection pool to ");
				s += connect_string_;
				throw std::runtime_error(s);
			}
			size_t n = std::thread::hardware_concurrency();
			LOG_INFO("Establishing %d -entry connection pool to:%s", connect_string_);

			pool_ = make_unique<soci::connection_pool>(n);
			for (size_t i = 0; i < n; ++i){
				LOG_DEBUG("Opening pool entry %d", i);
				soci::session& sess = pool_->at(i);
				sess.open(c);
				if (!IsSqlite()){
					SetSerializable(sess);
				}
			}
		}
		assert(pool_);
		return *pool_;
	}

	/*
	cache::lru_cache<std::string, std::shared_ptr<LedgerEntry const>>&
	Database::getEntryCache()
	{
	return mEntryCache;
	}*/

	class SQLLogContext : utils::NonCopyable
	{
		std::string mName;
		soci::session& mSess;
		std::ostringstream mCapture;

	public:
		SQLLogContext(std::string const& name, soci::session& sess)
			: mName(name), mSess(sess){
			mSess.set_log_stream(&mCapture);
		}

		~SQLLogContext(){
			mSess.set_log_stream(nullptr);
			std::string captured = mCapture.str();
			std::istringstream rd(captured);
			std::string buf;

			LOG_INFO("Database ");
			LOG_INFO("Database ");
			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database [SQL] begin capture: %s", mName);
			LOG_INFO("Database [SQL] -----------------------");


			while (std::getline(rd, buf)){
				LOG_INFO("Database [SQL] %s %s", mName, buf);
				buf.clear();
			}

			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database [SQL] end capture: %s", mName);
			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database ");
			LOG_INFO("Database ");
		}
	};

	StatementContext Database::GetPreparedStatement(std::string const& query){
		auto i = statements_.find(query);
		std::shared_ptr<soci::statement> p;
		if (i == statements_.end()){
			p = std::make_shared<soci::statement>(session_);
			p->alloc();
			p->prepare(query);
			statements_.insert(std::make_pair(query, p));
			//mStatementsSize.set_count(mStatements.size());
		}
		else{
			p = i->second;
		}
		StatementContext sc(p);
		return sc;
	}

	std::shared_ptr<SQLLogContext> Database::CaptureAndLogSQL(std::string contextName)	{
		return make_shared<SQLLogContext>(contextName, session_);
	}
}
