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

#include "socidb.h"
#include <main/configure.h>
#include <utils/logger.h>
#include <ledger/order_frm.h>

#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

extern "C" void register_factory_postgresql();
extern "C" void register_factory_sqlite3();


// NOTE: soci will just crash and not throw
//  if you misname a column in a query. yay!

namespace bumo
{

	using namespace soci;
	using namespace std;

	bool SociDb::gDriversRegistered = false;

	static void SetSerializable(soci::session& sess){
		sess << "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
			"SERIALIZABLE";
	}

	void SociDb::RegisterDrivers(const std::string& dbtype) {
		if (!gDriversRegistered){
			if (dbtype == "postgresql")
				register_factory_postgresql();
			else if(dbtype == "sqlite3")
				register_factory_sqlite3();
			gDriversRegistered = true;
		}
	}

	SociDb::SociDb() :pool_(NULL){}

	bool SociDb::Connect(const std::string& connect_string, const std::string& dbtype) {
		
		std::string connect_str;
		if (dbtype == "sqlite3"){
			connect_str = "sqlite3://" + connect_string;
		}
		else if (dbtype == "postgresql"){
			connect_str = "postgresql://" + connect_string;
		}
		else{
			error_desc_ = utils::String::Format("database type(%s) error", dbtype.c_str());
			return false;
		}

		RegisterDrivers(dbtype);

		try{
			session_.open(connect_str);
		}
		catch (soci_error const & e){
			error_desc_ = e.what();
			return false;
		}

		connect_string_ = connect_str;
		if (IsSqlite()){
			session_ << "PRAGMA journal_mode = WAL";
			// busy_timeout gives room for external processes
			// that may lock the database for some time
			session_ << "PRAGMA busy_timeout = 10000";
		}
		else{
			SetSerializable(session_);
		}
		return true;
	}

	void SociDb::Disconnect(){
		session_.close();
	}

	std::string SociDb::GetErrorDesc(){
		return error_desc_;
	}

	SociDb::~SociDb(){
		if (pool_ != NULL){
			delete pool_;
			pool_ = NULL;
		}
	}

	void SociDb::SetCurrentTransactionReadOnly(){
		if (!IsSqlite()){
			auto prep = GetPreparedStatement("SET TRANSACTION READ ONLY");
			auto& st = prep.statement();
			st.define_and_bind();
			st.execute(false);
		}
	}

	bool SociDb::IsSqlite() const	{
		return connect_string_.find("sqlite3:") !=
			std::string::npos;
	}

	bool SociDb::CanUsePool() const {
		return !(connect_string_ == ("sqlite3://:memory:"));
	}

	void SociDb::ClearPreparedStatementCache(){
		// Flush all prepared statements; in sqlite they represent open cursors
		// and will conflict with any DROP TABLE commands issued below
		for (auto st : statements_)	{
			st.second->clean_up(true);
		}
		statements_.clear();
		//mStatementsSize.set_count(mStatements.size());
	}

	void SociDb::Initialize(bool drop_data)	{
		ClearPreparedStatementCache();

		if (drop_data)
			OrderFrame::DropAll(*this);
		else
			OrderFrame::Initialize(*this);
	}

	soci::session& SociDb::GetSession(){
		// global session can only be used from the main thread
		//assertThreadIsMain();		// maybe err , todo
		return session_;
	}

	soci::connection_pool&	SociDb::GetPool()	{
		//if (!pool_){
		if (pool_ == NULL){
			if (!CanUsePool()){
				std::string s("Can't create connection pool to ");
				s += connect_string_;
				throw std::runtime_error(s);
			}
			size_t n = std::thread::hardware_concurrency();
			LOG_INFO("Establishing %d -entry connection pool to:%s", connect_string_.c_str());

			//pool_ = make_unique<soci::connection_pool>(n);
			pool_ = new soci::connection_pool(n);
			for (size_t i = 0; i < n; ++i){
				LOG_DEBUG("Opening pool entry %d", i);
				soci::session& sess = pool_->at(i);
				sess.open(connect_string_);
				if (!IsSqlite()){
					SetSerializable(sess);
				}
			}
		}
		//assert(pool_);
		assert(pool_ != NULL);
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
		std::string name_;
		soci::session& sess_;
		std::ostringstream capture_;

	public:
		SQLLogContext(std::string const& name, soci::session& sess)
			: name_(name), sess_(sess){
			sess_.set_log_stream(&capture_);
		}

		~SQLLogContext(){
			sess_.set_log_stream(nullptr);
			std::string captured = capture_.str();
			std::istringstream rd(captured);
			std::string buf;

			LOG_INFO("Database ");
			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database [SQL] begin capture: %s", name_.c_str());
			LOG_INFO("Database [SQL] -----------------------");


			while (std::getline(rd, buf)){
				LOG_INFO("Database [SQL] %s %s", name_.c_str(), buf.c_str());
				buf.clear();
			}

			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database [SQL] end capture: %s", name_.c_str());
			LOG_INFO("Database [SQL] -----------------------");
			LOG_INFO("Database ");

		}
	};

	StatementContext SociDb::GetPreparedStatement(std::string const& query){
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

	std::shared_ptr<SQLLogContext> SociDb::CaptureAndLogSQL(std::string contextName)	{
		return std::make_shared<SQLLogContext>(contextName, session_);
	}
}
