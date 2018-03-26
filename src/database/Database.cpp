// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "database/Database.h"

#include "../main/configure.h"
#include "utils/logger.h"
//#include "../ledger//OfferFrame.h"

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

static unsigned long const SCHEMA_VERSION = 5;

static void
setSerializable(soci::session& sess)
{
    sess << "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
            "SERIALIZABLE";
}

void
Database::registerDrivers()
{
    if (!gDriversRegistered)
    {
        register_factory_sqlite3();
#ifdef USE_POSTGRES
        register_factory_postgresql();
#endif
        gDriversRegistered = true;
    }
}

Database::Database(GlueManager& app)
    : mApp(app)
{
    registerDrivers();

	LOG_INFO("Connecting to: %s", bumo::Configure::Instance().db_configure_.sqlite_);		// todo


	mSession.open(bumo::Configure::Instance().db_configure_.sqlite_);
    if (isSqlite())
    {
        mSession << "PRAGMA journal_mode = WAL";
        // busy_timeout gives room for external processes
        // that may lock the database for some time
        mSession << "PRAGMA busy_timeout = 10000";
    }
    else
    {
        setSerializable(mSession);
    }
}

/*
void
Database::applySchemaUpgrade(unsigned long vers)
{
    clearPreparedStatementCache();

    switch (vers)
    {
    case 2:
        HerderPersistence::dropAll(*this);
        break;

    case 3:
        DataFrame::dropAll(*this);
        break;

    case 4:
        BanManager::dropAll(*this);
        mSession << "CREATE INDEX scpquorumsbyseq ON scpquorums(lastledgerseq)";
        break;

    case 5:
        try
        {
            mSession << "ALTER TABLE accountdata ADD lastmodified INT NOT NULL "
                        "DEFAULT 0;";
        }
        catch (soci::soci_error& e)
        {
            if (std::string(e.what()).find("lastmodified") == std::string::npos)
            {
                throw;
            }
        }
        break;

    default:
        throw std::runtime_error("Unknown DB schema version");
        break;
    }
}

void
Database::upgradeToCurrentSchema()
{
    auto vers = getDBSchemaVersion();
    if (vers > SCHEMA_VERSION)
    {
        std::string s = ("DB schema version " + std::to_string(vers) +
                         " is newer than application schema " +
                         std::to_string(SCHEMA_VERSION));
        throw std::runtime_error(s);
    }
    while (vers < SCHEMA_VERSION)
    {
        ++vers;
        CLOG(INFO, "Database")
            << "Applying DB schema upgrade to version " << vers;
        applySchemaUpgrade(vers);
        putSchemaVersion(vers);
    }
    assert(vers == SCHEMA_VERSION);
}

void
Database::putSchemaVersion(unsigned long vers)
{
    mApp.getPersistentState().setState(PersistentState::kDatabaseSchema,
                                       std::to_string(vers));
}


unsigned long
Database::getDBSchemaVersion()
{
    auto vstr =
        mApp.getPersistentState().getState(PersistentState::kDatabaseSchema);
    unsigned long vers = 0;
    try
    {
        vers = std::stoul(vstr);
    }
    catch (...)
    {
    }
    if (vers == 0)
    {
        throw std::runtime_error("No DB schema version found, try --newdb");
    }
    return vers;
}

unsigned long
Database::getAppSchemaVersion()
{
    return SCHEMA_VERSION;
}
*/
/*
medida::TimerContext
Database::getInsertTimer(std::string const& entityName)
{
    mEntityTypes.insert(entityName);
    mQueryMeter.Mark();
    return mApp.getMetrics()
        .NewTimer({"database", "insert", entityName})
        .TimeScope();
}

medida::TimerContext
Database::getSelectTimer(std::string const& entityName)
{
    mEntityTypes.insert(entityName);
    mQueryMeter.Mark();
    return mApp.getMetrics()
        .NewTimer({"database", "select", entityName})
        .TimeScope();
}

medida::TimerContext
Database::getDeleteTimer(std::string const& entityName)
{
    mEntityTypes.insert(entityName);
    mQueryMeter.Mark();
    return mApp.getMetrics()
        .NewTimer({"database", "delete", entityName})
        .TimeScope();
}

medida::TimerContext
Database::getUpdateTimer(std::string const& entityName)
{
    mEntityTypes.insert(entityName);
    mQueryMeter.Mark();
    return mApp.getMetrics()
        .NewTimer({"database", "update", entityName})
        .TimeScope();
}
*/
void
Database::setCurrentTransactionReadOnly()
{
    if (!isSqlite())
    {
        auto prep = getPreparedStatement("SET TRANSACTION READ ONLY");
        auto& st = prep.statement();
        st.define_and_bind();
        st.execute(false);
    }
}

bool
Database::isSqlite() const
{
	return bumo::Configure::Instance().db_configure_.sqlite_.find("sqlite3:") !=
           std::string::npos;
}

bool
Database::canUsePool() const
{
	return !(bumo::Configure::Instance().db_configure_.sqlite_ == ("sqlite3://:memory:"));
}

void
Database::clearPreparedStatementCache()
{
    // Flush all prepared statements; in sqlite they represent open cursors
    // and will conflict with any DROP TABLE commands issued below
    for (auto st : mStatements)
    {
        st.second->clean_up(true);
    }
    mStatements.clear();
    //mStatementsSize.set_count(mStatements.size());
}

void
Database::initialize()
{
    clearPreparedStatementCache();
 
    //OfferFrame::dropAll(*this);		// jin todo

}

soci::session&
Database::getSession()
{
    // global session can only be used from the main thread
    //assertThreadIsMain();		// maybe err , todo
    return mSession;
}

soci::connection_pool&
Database::getPool()
{
    if (!mPool)
    {
		auto const& c = bumo::Configure::Instance().db_configure_.sqlite_;
        if (!canUsePool())
        {
            std::string s("Can't create connection pool to ");
            s += bumo::Configure::Instance().db_configure_.sqlite_;
            throw std::runtime_error(s);
        }
        size_t n = std::thread::hardware_concurrency();
		LOG_INFO("Establishing %d -entry connection pool to:%s", bumo::Configure::Instance().db_configure_.sqlite_);

        mPool = make_unique<soci::connection_pool>(n);
        for (size_t i = 0; i < n; ++i)
        {
			LOG_DEBUG("Opening pool entry %d", i);
            soci::session& sess = mPool->at(i);
            sess.open(c);
            if (!isSqlite())
            {
                setSerializable(sess);
            }
        }
    }
    assert(mPool);
    return *mPool;
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
        : mName(name), mSess(sess)
    {
        mSess.set_log_stream(&mCapture);
    }
    ~SQLLogContext()
    {
        mSess.set_log_stream(nullptr);
        std::string captured = mCapture.str();
        std::istringstream rd(captured);
        std::string buf;

		LOG_INFO("Database ");
		LOG_INFO("Database ");
		LOG_INFO("Database [SQL] -----------------------");
		LOG_INFO("Database [SQL] begin capture: %s", mName);
		LOG_INFO("Database [SQL] -----------------------");


        while (std::getline(rd, buf))
        {
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

StatementContext
Database::getPreparedStatement(std::string const& query)
{
    auto i = mStatements.find(query);
    std::shared_ptr<soci::statement> p;
    if (i == mStatements.end())
    {
        p = std::make_shared<soci::statement>(mSession);
        p->alloc();
        p->prepare(query);
        mStatements.insert(std::make_pair(query, p));
        //mStatementsSize.set_count(mStatements.size());
    }
    else
    {
        p = i->second;
    }
    StatementContext sc(p);
    return sc;
}

std::shared_ptr<SQLLogContext>
Database::captureAndLogSQL(std::string contextName)
{
    return make_shared<SQLLogContext>(contextName, mSession);
}
/*
medida::Meter&
Database::getQueryMeter()
{
    return mQueryMeter;
}

std::chrono::nanoseconds
Database::totalQueryTime() const
{
    std::vector<std::string> qtypes = {"insert", "delete", "select", "update"};
    std::chrono::nanoseconds nsq(0);
    for (auto const& q : qtypes)
    {
        for (auto const& e : mEntityTypes)
        {
            auto& timer = mApp.getMetrics().NewTimer({"database", q, e});
            uint64_t sumns = static_cast<uint64_t>(
                timer.sum() *
                static_cast<double>(timer.duration_unit().count()));
            nsq += std::chrono::nanoseconds(sumns);
        }
    }
    return nsq;
}

void
Database::excludeTime(std::chrono::nanoseconds const& queryTime,
                      std::chrono::nanoseconds const& totalTime)
{
    mExcludedQueryTime += queryTime;
    mExcludedTotalTime += totalTime;
}

uint32_t
Database::recentIdleDbPercent()
{
    std::chrono::nanoseconds query = totalQueryTime();
    query -= mLastIdleQueryTime;
    query -= mExcludedQueryTime;

    std::chrono::nanoseconds total = mApp.getClock().now() - mLastIdleTotalTime;
    total -= mExcludedTotalTime;

    uint32_t queryPercent =
        static_cast<uint32_t>((100 * query.count()) / total.count());
    uint32_t idlePercent = 100 - queryPercent;
    if (idlePercent > 100)
    {
        // This should never happen, but clocks are not perfectly well behaved.
        CLOG(WARNING, "Database") << "DB idle percent (" << idlePercent
                                  << ") over 100, limiting to 100";
        idlePercent = 100;
    }

    CLOG(DEBUG, "Database") << "Estimated DB idle: " << idlePercent << "%"
                            << " (query=" << query.count() << "ns"
                            << ", total=" << total.count() << "ns)";

    mLastIdleQueryTime = totalQueryTime();
    mLastIdleTotalTime = mApp.getClock().now();
    mExcludedQueryTime = std::chrono::nanoseconds(0);
    mExcludedTotalTime = std::chrono::nanoseconds(0);
    return idlePercent;
}

DBTimeExcluder::DBTimeExcluder(Application& app)
    : mApp(app)
    , mStartQueryTime(app.getDatabase().totalQueryTime())
    , mStartTotalTime(app.getClock().now())
{
}

DBTimeExcluder::~DBTimeExcluder()
{
    auto deltaQ = mApp.getDatabase().totalQueryTime() - mStartQueryTime;
    auto deltaT = mApp.getClock().now() - mStartTotalTime;
    mApp.getDatabase().excludeTime(deltaQ, deltaT);
}
*/
}
