package core

import "time"

// GoTime represents go time provider
type GoTime struct{}

// NowUnix returns current unix timestamp
func (t GoTime) NowUnix() uint32 {
	return uint32(time.Now().UTC().Unix())
}
