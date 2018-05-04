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

#ifndef DATA_BASE_H
#define DATA_BASE_H




#include <utils/noncopyable.h>

#if defined(_MSC_VER)
#include <soci.h>
#else
#pragma GCC diagnostic push
// soci uses std::auto_ptr internally
// these warnings are useless to us and only clutters the output
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <soci.h>
#pragma GCC diagnostic pop
#endif // !

#include <set>
#include <string>
#include <map>


namespace bumo
{
	class LedgerManager;
	class SQLLogContext;

	/**
	 * Helper class for borrowing a SOCI prepared statement handle into a local
	 * scope and cleaning it up once done with it. Returned by
	 * Database::getPreparedStatement below.
	 */
	class StatementContext : utils::NonCopyable
	{
		std::shared_ptr<soci::statement> stmt_;

	public:
		StatementContext(std::shared_ptr<soci::statement> stmt) : stmt_(stmt){
			stmt_->clean_up(false);
		}
		StatementContext(StatementContext&& other){
			stmt_ = other.stmt_;
			other.stmt_.reset();
		}
		~StatementContext(){
			if (stmt_){
				stmt_->clean_up(false);
			}
		}
		soci::statement& statement(){
			return *stmt_;
		}
	};

	/**
	 * Object that owns the database connection(s) that an application
	 * uses to store the current ledger and other persistent state in.
	 *
	 * This may represent an in-memory SQLite instance (for testing), an on-disk
	 * SQLite instance (for running a minimal, self-contained server) or a
	 * connection to a local Postgresql database, that the node operator must have
	 * set up on their own.
	 *
	 * Database connects, on construction, to the target specified by the
	 * application Config object's Config::DATABASE value; this originates from the
	 * config-file's DATABASE string. The default is "sqlite3://:memory:". This
	 * "main connection" is where most SQL statements -- and all write-statements --
	 * are executed.
	 *
	 * Database may establish additional connections for worker threads to read
	 * data, from a separate connection pool, if worker threads request them. The
	 * pool will connect to the same target and only one connection will be made per
	 * worker thread.
	 *
	 * All database connections and transactions are set to snapshot isolation level
	 * (SQL isolation level 'SERIALIZABLE' in Postgresql and Sqlite, neither of
	 * which provide true serializability).
	 */
	class SociDb : utils::NonMovableOrCopyable
	{
		//medida::Meter& mQueryMeter;
		soci::session session_;
		//std::unique_ptr<soci::connection_pool> pool_;
		soci::connection_pool *pool_;

		std::string error_desc_;

		std::map<std::string, std::shared_ptr<soci::statement>> statements_;
		//medida::Counter& mStatementsSize;

		//cache::lru_cache<std::string, std::shared_ptr<LedgerEntry const>> mEntryCache;

		// Helpers for maintaining the total query time and calculating
		// idle percentage.
		std::set<std::string> mEntityTypes;
		//    std::chrono::nanoseconds mExcludedQueryTime;
		//    std::chrono::nanoseconds mExcludedTotalTime;
		//    std::chrono::nanoseconds mLastIdleQueryTime;
		//VirtualClock::time_point mLastIdleTotalTime;

		static bool gDriversRegistered;
		static void RegisterDrivers(const std::string& dbtype);
		//void applySchemaUpgrade(unsigned long vers);

		std::string connect_string_;
	public:
		// Instantiate object and connect to app.getConfig().DATABASE;
		// if there is a connection error, this will throw.
		SociDb();
		~SociDb();

		bool Connect(const std::string& connect_string, const std::string& dbtype);

		void Disconnect();
		std::string GetErrorDesc();

		// Return a crude meter of total queries to the db, for use in
		// overlay/LoadManager.
		//medida::Meter& getQueryMeter();

		// Number of nanoseconds spent processing queries since app startup,
		// without any reference to excluded time or running counters.
		// Strictly a sum of measured time.
		//std::chrono::nanoseconds totalQueryTime() const;

		// Subtract a number of nanoseconds from the running time counts,
		// due to database usage spikes, specifically during ledger-close.
		//void excludeTime(std::chrono::nanoseconds const& queryTime,
		//                 std::chrono::nanoseconds const& totalTime);

		// Return the percent of the time since the last call to this
		// method that database has been idle, _excluding_ the times
		// excluded above via `excludeTime`.
		//uint32_t recentIdleDbPercent();

		// Return a logging helper that will capture all SQL statements made
		// on the main connection while active, and will log those statements
		// to the process' log for diagnostics. For testing and perf tuning.
		std::shared_ptr<SQLLogContext> CaptureAndLogSQL(std::string contextName);

		// Return a helper object that borrows, from the Database, a prepared
		// statement handle for the provided query. The prepared statement handle
		// is ceated if necessary before borrowing, and reset (unbound from data)
		// when the statement context is destroyed.
		StatementContext GetPreparedStatement(std::string const& query);

		// Purge all cached prepared statements, closing their handles with the
		// database.
		void ClearPreparedStatementCache();

		// Return metric-gathering timers for various families of SQL operation.
		// These timers automatically count the time they are alive for,
		// so only acquire them immediately before executing an SQL statement.
		//medida::TimerContext getInsertTimer(std::string const& entityName);
		//medida::TimerContext getSelectTimer(std::string const& entityName);
		//medida::TimerContext getDeleteTimer(std::string const& entityName);
		//medida::TimerContext getUpdateTimer(std::string const& entityName);

		// If possible (i.e. "on postgres") issue an SQL pragma that marks
		// the current transaction as read-only. The effects of this last
		// only as long as the current SQL transaction.
		void SetCurrentTransactionReadOnly();

		// Return true if the Database target is SQLite, otherwise false.
		bool IsSqlite() const;

		// Return true if a connection pool is available for worker threads
		// to read from the database through, otherwise false.
		bool CanUsePool() const;

		void Initialize(bool drop_data=false);

		// Access the underlying SOCI session object
		soci::session& GetSession();

		// Access the optional SOCI connection pool available for worker
		// threads. Throws an error if !canUsePool().
		soci::connection_pool& GetPool();

		// Access the LedgerEntry cache. Note: clients are responsible for
		// invalidating entries in this cache as they perform statements
		// against the database. It's kept here only for ease of access.
		//typedef cache::lru_cache<std::string, std::shared_ptr<LedgerEntry const>> EntryCache;
		//EntryCache& getEntryCache();
	};
	/*
	class DBTimeExcluder : utils::NonCopyable
	{
	Application& mApp;
	std::chrono::nanoseconds mStartQueryTime;
	VirtualClock::time_point mStartTotalTime;

	public:
	DBTimeExcluder(Application& mApp);
	~DBTimeExcluder();
	}; */
}
#endif // !DATA_BASE_H
