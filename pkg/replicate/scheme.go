// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"path"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// BlockFilter is block filter that filters out compacted and unselected blocks.
type BlockFilter struct {
	logger           log.Logger
	labelSelector    labels.Selector
	resolutionLevels map[compact.ResolutionLevel]struct{}
	compactionLevels map[int]struct{}
}

// NewBlockFilter returns block filter.
func NewBlockFilter(
	logger log.Logger,
	labelSelector labels.Selector,
	resolutionLevels []compact.ResolutionLevel,
	compactionLevels []int,
) *BlockFilter {
	allowedResolutions := make(map[compact.ResolutionLevel]struct{})
	for _, resolutionLevel := range resolutionLevels {
		allowedResolutions[resolutionLevel] = struct{}{}
	}
	allowedCompactions := make(map[int]struct{})
	for _, compactionLevel := range compactionLevels {
		allowedCompactions[compactionLevel] = struct{}{}
	}
	return &BlockFilter{
		labelSelector:    labelSelector,
		logger:           logger,
		resolutionLevels: allowedResolutions,
		compactionLevels: allowedCompactions,
	}
}

// Filter return true if block is non-compacted and matches selector.
func (bf *BlockFilter) Filter(b *metadata.Meta) bool {
	if len(b.Thanos.Labels) == 0 {
		level.Error(bf.logger).Log("msg", "filtering block", "reason", "labels should not be empty")
		return false
	}

	blockLabels := labels.FromMap(b.Thanos.Labels)

	labelMatch := bf.labelSelector.Matches(blockLabels)
	if !labelMatch {
		selStr := "{"

		for i, m := range bf.labelSelector {
			if i != 0 {
				selStr += ","
			}

			selStr += m.String()
		}

		selStr += "}"

		level.Debug(bf.logger).Log("msg", "filtering block", "reason", "labels don't match", "block_labels", blockLabels.String(), "selector", selStr)

		return false
	}

	gotResolution := compact.ResolutionLevel(b.Thanos.Downsample.Resolution)
	if _, ok := bf.resolutionLevels[gotResolution]; !ok {
		level.Debug(bf.logger).Log("msg", "filtering block", "reason", "resolution doesn't match allowed resolutions", "got_resolution", gotResolution, "allowed_resolutions", bf.resolutionLevels)
		return false
	}

	gotCompactionLevel := b.BlockMeta.Compaction.Level
	if _, ok := bf.compactionLevels[gotCompactionLevel]; !ok {
		level.Debug(bf.logger).Log("msg", "filtering block", "reason", "compaction level doesn't match allowed levels", "got_compaction_level", gotCompactionLevel, "allowed_compaction_levels", bf.compactionLevels)
		return false
	}

	return true
}

type blockFilterFunc func(b *metadata.Meta) bool

// TODO: Add filters field.
type replicationScheme struct {
	fromBkt objstore.InstrumentedBucketReader
	toBkt   objstore.Bucket

	blockFilter blockFilterFunc
	fetcher     thanosblock.MetadataFetcher

	logger  log.Logger
	metrics *replicationMetrics

	reg prometheus.Registerer
}

type replicationMetrics struct {
	originIterations  prometheus.Counter
	originMetaLoads   prometheus.Counter
	originPartialMeta prometheus.Counter

	blocksAlreadyReplicated prometheus.Counter
	blocksReplicated        prometheus.Counter
	objectsReplicated       prometheus.Counter
}

func newReplicationMetrics(reg prometheus.Registerer) *replicationMetrics {
	m := &replicationMetrics{
		originIterations: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_origin_iterations_total",
			Help: "Total number of objects iterated over in the origin bucket.",
		}),
		originMetaLoads: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_origin_meta_loads_total",
			Help: "Total number of meta.json reads in the origin bucket.",
		}),
		originPartialMeta: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_origin_partial_meta_reads_total",
			Help: "Total number of partial meta reads encountered.",
		}),
		blocksAlreadyReplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_blocks_already_replicated_total",
			Help: "Total number of blocks skipped due to already being replicated.",
		}),
		blocksReplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_blocks_replicated_total",
			Help: "Total number of blocks replicated.",
		}),
		objectsReplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_replicate_objects_replicated_total",
			Help: "Total number of objects replicated.",
		}),
	}
	return m
}

func newReplicationScheme(
	logger log.Logger,
	metrics *replicationMetrics,
	blockFilter blockFilterFunc,
	fetcher thanosblock.MetadataFetcher,
	from objstore.InstrumentedBucketReader,
	to objstore.Bucket,
	reg prometheus.Registerer,
) *replicationScheme {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &replicationScheme{
		logger:      logger,
		blockFilter: blockFilter,
		fetcher:     fetcher,
		fromBkt:     from,
		toBkt:       to,
		metrics:     metrics,
		reg:         reg,
	}
}

func (rs *replicationScheme) execute(ctx context.Context) error {
	availableBlocks := []*metadata.Meta{}

	level.Debug(rs.logger).Log("msg", "scanning blocks available blocks for replication")

	if err := rs.fromBkt.Iter(ctx, "", func(name string) error {
		rs.metrics.originIterations.Inc()

		id, ok := thanosblock.IsBlockDir(name)
		if !ok {
			return nil
		}

		rs.metrics.originMetaLoads.Inc()

		meta, metaNonExistentOrPartial, err := loadMeta(ctx, rs, id)
		if metaNonExistentOrPartial {
			// meta.json is the last file uploaded by a Thanos shipper,
			// therefore a block may be partially present, but no meta.json
			// file yet. If this is the case we skip that block for now.
			rs.metrics.originPartialMeta.Inc()
			level.Info(rs.logger).Log("msg", "block meta not uploaded yet. Skipping.", "block_uuid", id.String())
			return nil
		}
		if err != nil {
			return errors.Wrapf(err, "load meta for block %v from origin bucket", id.String())
		}

		if len(meta.Thanos.Labels) == 0 {
			// TODO(bwplotka): Allow injecting custom labels as shipper does.
			level.Info(rs.logger).Log("msg", "block meta without Thanos external labels set. This is not allowed. Skipping.", "block_uuid", id.String())
			return nil
		}

		level.Debug(rs.logger).Log("msg", "adding block to available blocks", "block_uuid", id.String())

		availableBlocks = append(availableBlocks, meta)

		return nil
	}); err != nil {
		return errors.Wrap(err, "iterate over origin bucket")
	}

	candidateBlocks := []*metadata.Meta{}

	for _, b := range availableBlocks {
		if rs.blockFilter(b) {
			level.Debug(rs.logger).Log("msg", "adding block to candidate blocks", "block_uuid", b.BlockMeta.ULID.String())
			candidateBlocks = append(candidateBlocks, b)
		}
	}

	// In order to prevent races in compactions by the target environment, we
	// need to replicate oldest start timestamp first.
	sort.Slice(candidateBlocks, func(i, j int) bool {
		return candidateBlocks[i].BlockMeta.MinTime < candidateBlocks[j].BlockMeta.MinTime
	})

	for _, b := range candidateBlocks {
		if err := rs.ensureBlockIsReplicated(ctx, b.BlockMeta.ULID); err != nil {
			return errors.Wrapf(err, "ensure block %v is replicated", b.BlockMeta.ULID.String())
		}
	}

	return nil
}

// ensureBlockIsReplicated ensures that a block present in the origin bucket is
// present in the target bucket.
func (rs *replicationScheme) ensureBlockIsReplicated(ctx context.Context, id ulid.ULID) error {
	blockID := id.String()
	chunksDir := path.Join(blockID, thanosblock.ChunksDirname)
	indexFile := path.Join(blockID, thanosblock.IndexFilename)
	metaFile := path.Join(blockID, thanosblock.MetaFilename)

	level.Debug(rs.logger).Log("msg", "ensuring block is replicated", "block_uuid", blockID)

	originMetaFile, err := rs.fromBkt.ReaderWithExpectedErrs(rs.fromBkt.IsObjNotFoundErr).Get(ctx, metaFile)
	if err != nil {
		return errors.Wrap(err, "get meta file from origin bucket")
	}

	defer runutil.CloseWithLogOnErr(rs.logger, originMetaFile, "close original meta file")

	targetMetaFile, err := rs.toBkt.Get(ctx, metaFile)

	if targetMetaFile != nil {
		defer runutil.CloseWithLogOnErr(rs.logger, targetMetaFile, "close target meta file")
	}

	if err != nil && !rs.toBkt.IsObjNotFoundErr(err) && err != io.EOF {
		return errors.Wrap(err, "get meta file from target bucket")
	}

	originMetaFileContent, err := ioutil.ReadAll(originMetaFile)
	if err != nil {
		return errors.Wrap(err, "read origin meta file")
	}

	if targetMetaFile != nil && !rs.toBkt.IsObjNotFoundErr(err) {
		targetMetaFileContent, err := ioutil.ReadAll(targetMetaFile)
		if err != nil {
			return errors.Wrap(err, "read target meta file")
		}

		if bytes.Equal(originMetaFileContent, targetMetaFileContent) {
			// If the origin meta file content and target meta file content is
			// equal, we know we have already successfully replicated
			// previously.
			level.Debug(rs.logger).Log("msg", "skipping block as already replicated", "block_uuid", id.String())
			rs.metrics.blocksAlreadyReplicated.Inc()

			return nil
		}
	}

	if err := rs.fromBkt.Iter(ctx, chunksDir, func(objectName string) error {
		err := rs.ensureObjectReplicated(ctx, objectName)
		if err != nil {
			return errors.Wrapf(err, "replicate object %v", objectName)
		}

		return nil
	}); err != nil {
		return err
	}

	if err := rs.ensureObjectReplicated(ctx, indexFile); err != nil {
		return errors.Wrap(err, "replicate index file")
	}

	level.Debug(rs.logger).Log("msg", "replicating meta file", "object", metaFile)

	if err := rs.toBkt.Upload(ctx, metaFile, bytes.NewReader(originMetaFileContent)); err != nil {
		return errors.Wrap(err, "upload meta file")
	}

	rs.metrics.blocksReplicated.Inc()

	return nil
}

// ensureBlockIsReplicated ensures that an object present in the origin bucket
// is present in the target bucket.
func (rs *replicationScheme) ensureObjectReplicated(ctx context.Context, objectName string) error {
	level.Debug(rs.logger).Log("msg", "ensuring object is replicated", "object", objectName)

	exists, err := rs.toBkt.Exists(ctx, objectName)
	if err != nil {
		return errors.Wrapf(err, "check if %v exists in target bucket", objectName)
	}

	// skip if already exists.
	if exists {
		level.Debug(rs.logger).Log("msg", "skipping object as already replicated", "object", objectName)
		return nil
	}

	level.Debug(rs.logger).Log("msg", "object not present in target bucket, replicating", "object", objectName)

	r, err := rs.fromBkt.Get(ctx, objectName)
	if err != nil {
		return errors.Wrapf(err, "get %v from origin bucket", objectName)
	}

	defer r.Close()

	if err = rs.toBkt.Upload(ctx, objectName, r); err != nil {
		return errors.Wrapf(err, "upload %v to target bucket", objectName)
	}

	level.Info(rs.logger).Log("msg", "object replicated", "object", objectName)
	rs.metrics.objectsReplicated.Inc()

	return nil
}

// loadMeta loads the meta.json from the origin bucket and returns the meta
// struct as well as if failed, whether the failure was due to the meta.json
// not being present or partial. The distinction is important, as if missing or
// partial, this is just a temporary failure, as the block is still being
// uploaded to the origin bucket.
func loadMeta(ctx context.Context, rs *replicationScheme, id ulid.ULID) (*metadata.Meta, bool, error) {
	metas, _, err := rs.fetcher.Fetch(ctx)
	if err != nil {
		switch errors.Cause(err) {
		default:
			return nil, false, errors.Wrap(err, "fetch meta")
		case thanosblock.ErrorSyncMetaNotFound:
			return nil, true, errors.Wrap(err, "fetch meta")
		}
	}

	m, ok := metas[id]
	if !ok {
		return nil, true, errors.Wrap(err, "fetch meta")
	}

	return m, false, nil
}
