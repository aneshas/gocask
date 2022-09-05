package testutil

type Time uint32

func (t Time) NowUnix() uint32 {
	if t == 0 {
		return 12345
	}

	return uint32(t)
}
