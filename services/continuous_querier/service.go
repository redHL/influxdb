package continuous_querier

import (
	"errors"
	"fmt"
	//"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/qiniu/log.v1"
)

const (
	// When planning a select statement, passing zero tells it not to chunk results. Only applies to raw queries
	NoChunkingSize = 0
)

// ContinuousQuerier represents a service that executes continuous queries.
type ContinuousQuerier interface {
	// Run executes the named query in the named database.  Blank database or name matches all.
	Run(database, name string) error
}

// queryExecutor is an internal interface to make testing easier.
type queryExecutor interface {
	ExecuteQuery(query *influxql.Query, database string, chunkSize int) (<-chan *influxql.Result, error)
}

// metaStore is an internal interface to make testing easier.
type metaStore interface {
	IsLeader() bool
	Databases() ([]meta.DatabaseInfo, error)
	Database(name string) (*meta.DatabaseInfo, error)
}

// pointsWriter is an internal interface to make testing easier.
type pointsWriter interface {
	WritePoints(p *cluster.WritePointsRequest) error
}

// Service manages continuous query execution.
type Service struct {
	MetaStore     metaStore
	QueryExecutor queryExecutor
	PointsWriter  pointsWriter
	Config        *Config
	RunInterval   time.Duration
	// RunCh can be used by clients to signal service to run CQs.
	RunCh  chan struct{}
	Logger *log.Logger
	// lastRuns maps CQ name to last time it was run.
	lastRuns map[string]time.Time
	stop     chan struct{}
	wg       *sync.WaitGroup
}

func init() {
	log.SetOutputLevel(0)
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		Config:      &c,
		RunInterval: time.Second,
		RunCh:       make(chan struct{}),
		Logger:      log.New(os.Stderr, "[continuous_querier] ", log.LstdFlags),
		lastRuns:    map[string]time.Time{},
	}
	return s
}

// Open starts the service.
func (s *Service) Open() error {
	if s.stop != nil {
		return nil
	}

	assert(s.MetaStore != nil, "MetaStore is nil")
	assert(s.QueryExecutor != nil, "QueryExecutor is nil")
	assert(s.PointsWriter != nil, "PointsWriter is nil")

	s.stop = make(chan struct{})
	s.wg = &sync.WaitGroup{}
	s.wg.Add(1)
	go s.backgroundLoop()
	return nil
}

// Close stops the service.
func (s *Service) Close() error {
	if s.stop == nil {
		return nil
	}
	close(s.stop)
	s.wg.Wait()
	s.wg = nil
	s.stop = nil
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Run runs the specified continuous query, or all CQs if none is specified.
func (s *Service) Run(database, name string) error {
	var dbs []meta.DatabaseInfo

	if database != "" {
		// Find the requested database.
		db, err := s.MetaStore.Database(database)
		if err != nil {
			return err
		} else if db == nil {
			return tsdb.ErrDatabaseNotFound(database)
		}
		dbs = append(dbs, *db)
	} else {
		// Get all databases.
		var err error
		dbs, err = s.MetaStore.Databases()
		if err != nil {
			return err
		}
	}

	// Loop through databases.
	for _, db := range dbs {
		// Loop through CQs in each DB executing the ones that match name.
		for _, cq := range db.ContinuousQueries {
			if name == "" || cq.Name == name {
				// Reset the last run time for the CQ.
				s.lastRuns[cq.Name] = time.Time{}
			}
		}
	}

	// Signal the background routine to run CQs.
	s.RunCh <- struct{}{}

	return nil
}

// backgroundLoop runs on a go routine and periodically executes CQs.
func (s *Service) backgroundLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case <-s.RunCh:
			if s.MetaStore.IsLeader() {
				s.Logger.Print("running continuous queries by request")
				s.runContinuousQueries()
			}
		case <-time.After(s.RunInterval):
			log.Debug("s.RunInterval=", s.RunInterval)
			if s.MetaStore.IsLeader() {
				s.runContinuousQueries()
			}
		}
	}
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQueries() {
	log.Debug("runContinuousQueries()!")
	// Get list of all databases.
	dbs, err := s.MetaStore.Databases()
	if err != nil {
		s.Logger.Println("error getting databases")
		return
	}
	// Loop through all databases executing CQs.
	for _, db := range dbs {
		// TODO: distribute across nodes
		for _, cq := range db.ContinuousQueries {
			log.Debug("db=", db.Name, " cq=", cq)
			if err := s.ExecuteContinuousQuery(&db, &cq); err != nil {
				s.Logger.Printf("error executing query: %s: err = %s", cq.Query, err)
			}
		}
	}
}

// ExecuteContinuousQuery executes a single CQ.
func (s *Service) ExecuteContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo) error {
	log.Debug("ExecuteContinuousQuery(dbi *meta.DatabaseInfo, cqi *meta.ContinuousQueryInfo)!")
	log.Debug("dbi=", dbi, " cqi=", cqi)
	// TODO: re-enable stats
	//s.stats.Inc("continuousQueryExecuted")

	// Local wrapper / helper.
	cq, err := NewContinuousQuery(dbi.Name, cqi)
	if err != nil {
		return err
	}

	// Get the last time this CQ was run from the service's cache.
	cq.LastRun = s.lastRuns[cqi.Name]

	// Set the retention policy to default if it wasn't specified in the query.
	if cq.intoRP() == "" {
		cq.setIntoRP(dbi.DefaultRetentionPolicy)
	}

	// See if this query needs to be run.
	computeNoMoreThan := time.Duration(s.Config.ComputeNoMoreThan)
	run, err := cq.shouldRunContinuousQuery(s.Config.ComputeRunsPerInterval, computeNoMoreThan)
	log.Debug("run=", run)
	if err != nil {
		return err
	} else if !run {
		return nil
	}

	// We're about to run the query so store the time.
	now := time.Now()
	cq.LastRun = now
	s.lastRuns[cqi.Name] = now

	// Get the group by interval.
	interval, err := cq.q.GroupByInterval()
	if err != nil {
		return err
	} else if interval == 0 {
		return nil
	}

	// Calculate and set the time range for the query.
	startTime := now.Round(interval)
	if startTime.UnixNano() > now.UnixNano() {
		startTime = startTime.Add(-interval)
	}

	if err := cq.q.SetTimeRange(startTime, startTime.Add(interval)); err != nil {
		s.Logger.Printf("error setting time range: %s\n", err)
	}

	// Do the actual processing of the query & writing of results.
	if err := s.runContinuousQueryAndWriteResult(cq); err != nil {
		s.Logger.Printf("error: %s. running: %s\n", err, cq.q.String())
		return err
	}

	recomputeNoOlderThan := time.Duration(s.Config.RecomputeNoOlderThan)

	for i := 0; i < s.Config.RecomputePreviousN; i++ {
		log.Debug("i=", i, " s.Config.RecomputePreviousN=", s.Config.RecomputePreviousN)
		// if we're already more time past the previous window than we're going to look back, stop
		if now.Sub(startTime) > recomputeNoOlderThan {
			return nil
		}
		newStartTime := startTime.Add(-interval)

		if err := cq.q.SetTimeRange(newStartTime, startTime); err != nil {
			s.Logger.Printf("error setting time range: %s\n", err)
			return err
		}

		if err := s.runContinuousQueryAndWriteResult(cq); err != nil {
			s.Logger.Printf("error during recompute previous: %s. running: %s\n", err, cq.q.String())
			return err
		}

		startTime = newStartTime
	}
	return nil
}

// runContinuousQueryAndWriteResult will run the query against the cluster and write the results back in
func (s *Service) runContinuousQueryAndWriteResult(cq *ContinuousQuery) error {
	log.Debug("runContinuousQueryAndWriteResult(cq *ContinuousQuery)! cq=", cq)
	// Wrap the CQ's inner SELECT statement in a Query for the QueryExecutor.
	q := &influxql.Query{
		Statements: influxql.Statements{cq.q},
	}

	// Execute the SELECT.
	ch, err := s.QueryExecutor.ExecuteQuery(q, cq.Database, NoChunkingSize)
	if err != nil {
		log.Debug("err=", err)
		return err
	}

	// Read all rows from the result channel.
	for result := range ch {
		log.Debug("result=", result)
		if result.Err != nil {
			return result.Err
		}

		for _, row := range result.Series {
			log.Debug("row=", row.Tags, row.Values)
			// Convert the result row to points.
			points, err := s.convertRowToPoints(cq.intoMeasurement(), row)
			if err != nil {
				log.Println(err)
				continue
			}

			if len(points) == 0 {
				continue
			}

			// If the points have any nil values, can't write.
			// This happens if the CQ is created and running before data is written to the measurement.
			for _, p := range points {
				fields := p.Fields()
				for _, v := range fields {
					if v == nil {
						return nil
					}
				}
			}

			// Create a write request for the points.
			req := &cluster.WritePointsRequest{
				Database:         cq.intoDB(),
				RetentionPolicy:  cq.intoRP(),
				ConsistencyLevel: cluster.ConsistencyLevelAny,
				Points:           points,
			}

			// Write the request.
			if err := s.PointsWriter.WritePoints(req); err != nil {
				s.Logger.Println(err)
				return err
			}
		}
	}

	return nil
}

// convertRowToPoints will convert a query result Row into Points that can be written back in.
// Used for continuous and INTO queries
func (s *Service) convertRowToPoints(measurementName string, row *influxql.Row) ([]tsdb.Point, error) {
	log.Debug("convertRowToPoints()!")
	// figure out which parts of the result are the time and which are the fields
	timeIndex := -1
	fieldIndexes := make(map[string]int)
	log.Debug("--------------------------------------------------1+++++++++++++++++++++")
	for i, c := range row.Columns {
		log.Debug("i=", i, " c=", c)
		if c == "time" {
			timeIndex = i
		} else {
			fieldIndexes[c] = i
		}
	}
	log.Debug("--------------------------------------------------2+++++++++++++++++++++")

	if timeIndex == -1 {
		return nil, errors.New("error finding time index in result")
	}

	log.Debug("--------------------------------------------------3+++++++++++++++++++++")
	points := make([]tsdb.Point, 0, len(row.Values))
	for _, v := range row.Values {
		log.Debug("--------------------------------------------------4+++++++++++++++++++++")
		log.Debug("v=", v, " len(v)=", len(v), " len(fieldIndexes)=", len(fieldIndexes))
		vals := make(map[string]interface{})
		if len(v)-1 != len(fieldIndexes) {
			return nil, fmt.Errorf("unknown field or tag name in select clause")
		}
		for fieldName, fieldIndex := range fieldIndexes {
			log.Debug("fieldName=", fieldName, " fieldIndex=", fieldIndex)
			vals[fieldName] = v[fieldIndex]
			log.Debug("v[fieldIndex]=", v[fieldIndex])
		}

		log.Debug("--------------------------------------------------5+++++++++++++++++++++")
		p := tsdb.NewPoint(measurementName, row.Tags, vals, v[timeIndex].(time.Time))
		log.Debug("--------------------------------------------------6+++++++++++++++++++++")
		points = append(points, p)
	}

	return points, nil
}

// ContinuousQuery is a local wrapper / helper around continuous queries.
type ContinuousQuery struct {
	Database string
	Info     *meta.ContinuousQueryInfo
	LastRun  time.Time
	q        *influxql.SelectStatement
}

func (cq *ContinuousQuery) intoDB() string          { return cq.q.Target.Measurement.Database }
func (cq *ContinuousQuery) intoRP() string          { return cq.q.Target.Measurement.RetentionPolicy }
func (cq *ContinuousQuery) setIntoRP(rp string)     { cq.q.Target.Measurement.RetentionPolicy = rp }
func (cq *ContinuousQuery) intoMeasurement() string { return cq.q.Target.Measurement.Name }

// NewContinuousQuery returns a ContinuousQuery object with a parsed influxql.CreateContinuousQueryStatement
func NewContinuousQuery(database string, cqi *meta.ContinuousQueryInfo) (*ContinuousQuery, error) {
	log.Debug("NewContinuousQuery()!")
	stmt, err := influxql.NewParser(strings.NewReader(cqi.Query)).ParseStatement()
	if err != nil {
		return nil, err
	}

	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)

	if !ok || q.Source.Target == nil || q.Source.Target.Measurement == nil {
		return nil, errors.New("query isn't a valid continuous query")
	}

	cquery := &ContinuousQuery{
		Database: database,
		Info:     cqi,
		q:        q.Source,
	}

	return cquery, nil
}

// shouldRunContinuousQuery returns true if the CQ should be schedule to run. It will use the
// lastRunTime of the CQ and the rules for when to run set through the config to determine
// if this CQ should be run
func (cq *ContinuousQuery) shouldRunContinuousQuery(runsPerInterval int, noMoreThan time.Duration) (bool, error) {
	log.Debug("shouldRunContinuousQuery()!")
	// if it's not aggregated we don't run it
	if cq.q.IsRawQuery {
		return false, errors.New("continuous queries must be aggregate queries")
	}

	// since it's aggregated we need to figure how often it should be run
	interval, err := cq.q.GroupByInterval()
	if err != nil {
		return false, err
	}

	// determine how often we should run this continuous query.
	// group by time / the number of times to compute
	computeEvery := time.Duration(interval.Nanoseconds()/int64(runsPerInterval)) * time.Nanosecond
	// make sure we're running no more frequently than the setting in the config
	if computeEvery < noMoreThan {
		computeEvery = noMoreThan
	}

	// if we've passed the amount of time since the last run, do it up
	if cq.LastRun.Add(computeEvery).UnixNano() <= time.Now().UnixNano() {
		return true, nil
	}

	return false, nil
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
