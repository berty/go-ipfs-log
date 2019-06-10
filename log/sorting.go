package log

import (
	"bytes"
	"errors"
	"github.com/berty/go-ipfs-log/entry"
	"github.com/berty/go-ipfs-log/utils/lamportclock"
)

func SortByClocks(a, b *entry.Entry, resolveConflict func(a *entry.Entry, b *entry.Entry) (int, error)) (int, error) {
	diff := lamportclock.Compare(a.Clock, b.Clock)

	if diff == 0 {
		return resolveConflict(a, b)
	}

	return diff, nil
}

func SortByClockId(a, b *entry.Entry, resolveConflict func(a *entry.Entry, b *entry.Entry) (int, error)) (int, error) {
	comparedIDs := bytes.Compare(a.Clock.ID, b.Clock.ID)

	if comparedIDs == 0 {
		return resolveConflict(a, b)
	}
	if comparedIDs < 0 {
		return -1, nil
	}

	return 1, nil
}

func First (a, b *entry.Entry) (int, error) {
	return 1, nil
}

func FirstWriteWins(a, b *entry.Entry) (int, error) {
	res, err := LastWriteWins(a, b)

	return res * -1, err
}

func LastWriteWins(a, b *entry.Entry) (int, error) {
	sortByID := func(a *entry.Entry, b *entry.Entry) (int, error) {
		return SortByClockId(a, b, First)
	}

	sortByEntryClocks := func(a *entry.Entry, b *entry.Entry) (int, error) {
		return SortByClocks(a, b, sortByID)
	}

	return sortByEntryClocks(a, b)
}


func NoZeroes (compFunc func (a, b *entry.Entry) (int, error)) func (a, b *entry.Entry) (int, error) {
	return func (a, b *entry.Entry) (int, error) {
		ret, err := compFunc(a, b)
		if ret != 0 || err != nil {
			return ret, err
		}

		return 0, errors.New(`err: Your log's tiebreaker function has returned zero and therefore cannot be`)
	}
}

func Reverse(a []*entry.Entry) {
	for i := len(a)/2-1; i >= 0; i-- {
		opp := len(a)-1-i
		a[i], a[opp] = a[opp], a[i]
	}
}
