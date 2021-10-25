// Package ttl provides the time-to-live enforcement service.
package ttl

import (
	"sync"
	"time"

	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/cnosdatabase/db/logger"
	"go.uber.org/zap"
)

// Service represents the time-to-live enforcement service.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		DeleteRegion(database, ttl string, id uint64) error
		PruneRegions() error
	}
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	config Config
	wg     sync.WaitGroup
	done   chan struct{}

	logger *zap.Logger
}

// NewService returns a configured time-to-live enforcement service.
func NewService(c Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
	}
}

// Open starts time-to-live enforcement.
func (s *Service) Open() error {
	if !s.config.Enabled || s.done != nil {
		return nil
	}

	s.logger.Info("Starting time-to-live enforcement service",
		logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	return nil
}

// Close stops time-to-live enforcement.
func (s *Service) Close() error {
	if !s.config.Enabled || s.done == nil {
		return nil
	}

	s.logger.Info("Closing time-to-live enforcement service")
	close(s.done)

	s.wg.Wait()
	s.done = nil
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "ttl"))
}

func (s *Service) run() {
	ticker := time.NewTicker(time.Duration(s.config.CheckInterval))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			log, logEnd := logger.NewOperation(s.logger, "Time-to-live deletion check", "ttl_delete_check")

			type deletionInfo struct {
				db  string
				ttl string
			}
			deletedShardIDs := make(map[uint64]deletionInfo)

			// Mark down if an error occurred during this function so we can inform the
			// user that we will try again on the next interval.
			// Without the message, they may see the error message and assume they
			// have to do it manually.
			var retryNeeded bool
			dbs := s.MetaClient.Databases()
			for _, d := range dbs {
				for _, r := range d.TimeToLives {
					// Build list of already deleted shards.
					for _, g := range r.DeletedRegions() {
						for _, sh := range g.Shards {
							deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, ttl: r.Name}
						}
					}

					// Determine all shards that have expired and need to be deleted.
					for _, g := range r.ExpiredRegions(time.Now().UTC()) {
						if err := s.MetaClient.DeleteRegion(d.Name, r.Name, g.ID); err != nil {
							log.Info("Failed to delete region",
								logger.Database(d.Name),
								logger.Region(g.ID),
								logger.TimeToLive(r.Name),
								zap.Error(err))
							retryNeeded = true
							continue
						}

						log.Info("Deleted region",
							logger.Database(d.Name),
							logger.Region(g.ID),
							logger.TimeToLive(r.Name))

						// Store all the shard IDs that may possibly need to be removed locally.
						for _, sh := range g.Shards {
							deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, ttl: r.Name}
						}
					}
				}
			}

			// Remove shards if we store them locally
			for _, id := range s.TSDBStore.ShardIDs() {
				if info, ok := deletedShardIDs[id]; ok {
					if err := s.TSDBStore.DeleteShard(id); err != nil {
						log.Info("Failed to delete shard",
							logger.Database(info.db),
							logger.Shard(id),
							logger.TimeToLive(info.ttl),
							zap.Error(err))
						retryNeeded = true
						continue
					}
					log.Info("Deleted shard",
						logger.Database(info.db),
						logger.Shard(id),
						logger.TimeToLive(info.ttl))
				}
			}

			if err := s.MetaClient.PruneRegions(); err != nil {
				log.Info("Problem pruning regions", zap.Error(err))
				retryNeeded = true
			}

			if retryNeeded {
				log.Info("One or more errors occurred during shard deletion and will be retried on the next check", logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
			}

			logEnd()
		}
	}
}
