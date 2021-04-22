package ownmapdal

type Rescan struct {
	WayRescanMap, RelationRescanMap *RescanMap
}

func NewRescan() *Rescan {
	return &Rescan{
		NewRescanMap(),
		NewRescanMap(),
	}
}

func (r *Rescan) MarkNewIteration() {
	r.WayRescanMap.rescanRequestCountThisIteration = 0
	r.RelationRescanMap.rescanRequestCountThisIteration = 0
	for _, rescanMap := range []rescanInnerMap{r.WayRescanMap.m, r.RelationRescanMap.m} {
		for id, rescanResult := range rescanMap {
			if rescanResult.hasBeenAnIterationResetSinceAdded == true {
				// object has been in the "rescan" list for at least once pass, and has not been picked up. Therefore we know it's out of bounds/not in the import file
				rescanResult.KnownToBeOutOfBounds = true
			}

			rescanResult.hasBeenAnIterationResetSinceAdded = true
			rescanMap[id] = rescanResult
		}
	}
}
func (r *Rescan) GetRescanItemRequestsThisIteration() int64 {
	return r.WayRescanMap.rescanRequestCountThisIteration + r.RelationRescanMap.rescanRequestCountThisIteration
}

type RescanResult struct {
	KnownToBeOutOfBounds              bool
	hasBeenAnIterationResetSinceAdded bool
}

type rescanInnerMap map[int64]RescanResult

type RescanMap struct {
	m                               rescanInnerMap
	rescanRequestCountThisIteration int64
}

func NewRescanMap() *RescanMap {
	return &RescanMap{
		make(rescanInnerMap),
		0,
	}
}

// RequestWayIDToBeRescanned marks a Way ID to be rescanned
func (r *RescanMap) RequestIDToBeRescanned(id int64) {
	_, exists := r.m[id]
	if exists {
		// nothing to do. Already marked for rescan
		return
	}
	r.m[id] = RescanResult{}
	r.rescanRequestCountThisIteration++
}
func (r *RescanMap) MarkIDAsScannedButNotInBounds(id int64) {
	rescanResult, ok := r.m[id]
	if !ok {
		// object not requested (yet), so do nothing more
		return
	}

	rescanResult.KnownToBeOutOfBounds = true
	r.m[id] = rescanResult
}
func (r *RescanMap) GetValueOfWayMarkedForRescan(id int64) RescanResult {
	return r.m[id]
}
func (r *RescanMap) IsItemRequestedForRescan(id int64) bool {
	_, ok := r.m[id]
	return ok
}
